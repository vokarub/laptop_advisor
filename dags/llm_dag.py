import os
import logging
from pathlib import Path
from langchain_ollama import OllamaLLM
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Настройки LLM
OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://localhost:11434")
llm = OllamaLLM(
    model='gemma3:1b',
    base_url=OLLAMA_HOST,
    temperature=0.2,
)

# Настройка логирования
logging.basicConfig(
    filename='/opt/airflow/logs/process_reviews.log',  # Путь к файлу логов
    level=logging.INFO,  # Уровень логирования
    format='%(asctime)s - %(levelname)s - %(message)s',  # Формат логов
)

# Функции обработки
def read_file(path):
    with open(path, 'r', encoding='utf-8') as f:
        return f.read()

def write_file(path, content):
    with open(path, 'w', encoding='utf-8') as f:
        f.write(content)

def call_local_llm(prompt):
    response = llm.invoke(input=prompt)
    return response.strip()

def aggregate_reviews(raw_reviews):
    prompt = f"""
Ты — помощник, который анализирует отзывы пользователей о ноутбуке.
На основе приведённого ниже текста, выдели:

1. Общие достоинства (кратко, в списке).
2. Общие недостатки (кратко, в списке).
3. Общий комментарий (кратко, в списке)

Отзывы пользователей:
{raw_reviews}

Выведи результат в следующем формате:
Достоинства
- ...
- ...

Недостатки
- ...
- ...

Комментарий
- ...
- ...
"""
    return call_local_llm(prompt)

# Основная логика задачи
def process_reviews():
    root_dir = os.getenv("REVIEWS_ROOT", "/opt/airflow/")
    input_dir = os.path.join(root_dir, "opinion_dump")
    output_dir = os.path.join(root_dir, "laptop_markdown_files")

    os.makedirs(output_dir, exist_ok=True)

    for filename in os.listdir(input_dir):
        if filename.endswith(".md"):
            input_path = os.path.join(input_dir, filename)
            output_path = os.path.join(output_dir, filename)

            raw_reviews = read_file(input_path)
            summary = aggregate_reviews(raw_reviews)
            write_file(output_path, summary)

            # Логируем обработку файла
            logging.info(f"Файл {filename} обработан и сохранён в {output_path}")

            # Удаляем оригинальный файл после обработки
            os.remove(input_path)

            # Логируем удаление файла
            logging.info(f"Файл {filename} удалён из {input_path}")

# Параметры DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 6, 15),
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    dag_id='process_laptop_reviews',
    default_args=default_args,
    schedule_interval='0 6 * * *',  # Каждый день в 6:00 утра
    catchup=False,
    description='Анализ отзывов о ноутбуках каждый день в 6 утра',
)

task = PythonOperator(
    task_id='aggregate_laptop_reviews',
    python_callable=process_reviews,
    dag=dag,
)
