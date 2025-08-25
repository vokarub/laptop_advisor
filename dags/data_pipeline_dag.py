import pandas as pd
import re
import ast
from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# --- Логика для первой задачи (из excel_to_csv.py) ---
# Эта функция будет выполняться первым таском.
# ВАЖНО: Убедитесь, что файлы 'info_dump_daily.xlsx', 'util/cpu_scores.csv' и 'util/gpu_scores.csv'
# доступны для Airflow worker'а по указанным путям.
def excel_to_df_callable():
    """
    Читает исходный Excel, обрабатывает данные и возвращает pandas DataFrame.
    Этот DataFrame будет передан следующей задаче через XCom.
    """
    input_excel = '/opt/airflow/dags/data/info_dump_daily.xlsx'  # <-- Пример пути в Docker
    cpu_scores_path = '/opt/airflow/dags/data/util/cpu_scores.csv'
    gpu_scores_path = '/opt/airflow/dags/data/util/gpu_scores.csv'

    df = pd.read_excel(input_excel)
    cpu_df = pd.read_csv(cpu_scores_path)
    gpu_df = pd.read_csv(gpu_scores_path)

    def get_score(scores_df: pd.DataFrame, model_name: str) -> int:
        if not model_name or pd.isna(model_name):
            return 0
        result = scores_df[scores_df['model'].str.lower() == model_name.lower()]
        if not result.empty:
            return int(result['score'].values[0])
        return 0

    def calculate_total_score(cpu_model: str, gpu_inserted: str, gpu_discrete:str, ram_size: int, ssd_size: int, all_cores: int) -> int:
        cpu_score = get_score(cpu_df, cpu_model)
        gpu_inserted_score = get_score(gpu_df, gpu_inserted)
        gpu_discrete_score = get_score(gpu_df, gpu_discrete)
        gpu_score = gpu_inserted_score + gpu_discrete_score
        return (cpu_score + gpu_score + ram_size*100 + ssd_size + all_cores*100)/100

    def clean_number(value, remove_chars=None):
        if not isinstance(value, str):
            return value
        if remove_chars:
            for char in remove_chars:
                value = value.replace(char, '')
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    output_rows = []
    for index, row in df.iterrows():
        try:
            chars = ast.literal_eval(row['chars'])
            char_dict = {key: value for key, value in chars}

            ram_size = int(clean_number(char_dict.get('Объем оперативной памяти', '0'), remove_chars=['ГБ']))
            ssd_size = int(clean_number(char_dict.get('Общий объем твердотельных накопителей (SSD)', '0'), remove_chars=['ГБ']))
            p_cores = int(char_dict.get('Количество производительных ядер', 0))
            e_cores = int(char_dict.get('Количество энергоэффективных ядер', 0))
            all_cores = p_cores + e_cores
            cpu = char_dict.get('Модель процессора', '')
            gpu_inserted = char_dict.get('Модель встроенной видеокарты', '')
            gpu_discrete = char_dict.get('Модель дискретной видеокарты', '').replace('для ноутбуков', "").strip()

            output_row = {
                'id': row['uuid'],
                'name': row['name'].replace('"', '').replace('/',""),
                'model': char_dict.get('Модель', ''),
                'price': int(re.sub(r'₽\d*', '', str(row['price'])).strip()),
                'os_name': char_dict.get('Операционная система', ""),
                'is_gaming': 1 if char_dict.get('Игровой ноутбук', '').strip().lower() == 'есть' else 0,
                'color': char_dict.get('Цвет верхней крышки', ""),
                'material': char_dict.get('Материал корпуса', ""),
                'construction': char_dict.get('Конструктивное исполнение', ''),
                'usb_a': char_dict.get("Разъемы USB Type-A", ''),
                'usb_c': char_dict.get("Разъемы USB Type-C", ''), # Исправлен ключ для Type-C
                'video_slots': char_dict.get('Видеоразъемы', ''),
                'touch_screen': char_dict.get('Сенсорный экран', 'нет'),
                'screen_type': char_dict.get('Тип экрана', ''),
                'diagonal': clean_number(char_dict.get('Диагональ экрана\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t(дюйм)', '').replace('"', ''), remove_chars=['"']),
                'resolution': char_dict.get('Разрешение экрана', ''),
                'refresh_rate': clean_number(char_dict.get("Максимальная частота обновления экрана", ''), remove_chars=['Гц']),
                'cpu': cpu,
                'p_cores': p_cores,
                'e_cores': e_cores,
                'ram_size': ram_size,
                'ram_slots': clean_number(char_dict.get('Свободные слоты для оперативной памяти', '0')),
                'gpu_inserted': gpu_inserted,
                'gpu_discrete': gpu_discrete,
                'ssd_size': ssd_size,
                'ssd_slots': char_dict.get('Свободные слоты для накопителей', 'нет'),
                'battery_time': clean_number(char_dict.get('Приблизительное время автономной работы', ''), remove_chars=['ч']),
                'weight': clean_number(char_dict.get('Вес', ''), remove_chars=['кг', ',']),
                'score': calculate_total_score(cpu, gpu_inserted, gpu_discrete, ram_size, ssd_size, all_cores)
            }
            output_rows.append(output_row)
        except Exception as e:
            print(f"Ошибка обработки строки {index}: {e}")

    output_df = pd.DataFrame(output_rows)
    return output_df


# --- Логика для второй задачи (из csv_to_postgres.py) ---
# Эта функция будет выполняться вторым таском.
def df_to_postgres_callable(**kwargs):
    """
    Получает DataFrame из XCom, подключается к Postgres через Hook
    и загружает данные.
    """
    # 1. Получаем DataFrame из XCom, который вернула предыдущая задача
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='task_excel_to_df')

    if df is None or df.empty:
        print("Не получено данных от предыдущей задачи. Пропуск.")
        return

    # 2. Используем Airflow Connection для безопасного подключения к БД
    #    Вам нужно создать подключение с ID 'laptops_db_conn' в UI Airflow
    hook = PostgresHook(postgres_conn_id='laptops_db_conn')
    engine = hook.get_sqlalchemy_engine()

    # 3. Эффективно загружаем данные в таблицу 'laptops'
    #    Таблица и колонки должны существовать.
    #    'append' добавляет данные. 'replace' перезаписывает таблицу.
    df.to_sql('laptops', con=engine, if_exists='replace', index=False)
    print(f"Успешно загружено {len(df)} строк в таблицу laptops.")


# --- Определение DAG ---
with DAG(
    dag_id='laptops_processing_pipeline',
    default_args={
        'owner': 'airflow',
        'retries': 2,
        'retry_delay': timedelta(minutes=2)
    },
    description='Daily data processing pipeline from Excel to Postgres',
    start_date=datetime(2025, 6, 9),
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
) as dag:

    task_excel_to_df = PythonOperator(
        task_id='task_excel_to_df',
        python_callable=excel_to_df_callable,
    )

    task_df_to_postgres = PythonOperator(
        task_id='task_df_to_postgres',
        python_callable=df_to_postgres_callable,
    )

    # Определяем последовательность выполнения задач
    task_excel_to_df >> task_df_to_postgres