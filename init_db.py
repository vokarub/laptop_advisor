import csv
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

# Настройки подключения к Postgres
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "laptops_db",
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD")
}

# Путь к файлу CSV
CSV_FILE = 'output_db.csv'

def create_table(conn):
    with conn.cursor() as cur:
        cur.execute('''
            DROP TABLE IF EXISTS laptops;
            CREATE TABLE laptops (
                id UUID PRIMARY KEY,
                name TEXT,
                model TEXT,
                price INTEGER,
                os_name TEXT,
                is_gaming INTEGER,
                color TEXT,
                material TEXT,
                construction TEXT,
                usb_a TEXT,
                usb_c TEXT,
                video_slots TEXT,
                touch_screen TEXT,
                kb_light TEXT,
                screen_type TEXT,
                diagonal REAL,
                resolution TEXT,
                refresh_rate INTEGER,
                cpu TEXT,
                p_cores INTEGER,
                e_cores INTEGER,
                ram_size INTEGER,
                ram_slots INTEGER,
                gpu_inserted TEXT,
                gpu_discrete TEXT,
                ssd_size INTEGER,
                ssd_slots TEXT,
                battery_time REAL,
                weight REAL,
                score REAL
            );
        ''')
    conn.commit()
    print("Таблица laptops создана.")

def insert_data(conn, csv_file):
    with open(csv_file, encoding='utf-8') as f:
        reader = csv.DictReader(f)
        print("Заголовки CSV:", reader.fieldnames)
        with conn.cursor() as cur:
            for row in reader:
                cur.execute('''
                    INSERT INTO laptops (
                        id, name, model, price, os_name, is_gaming, color, material, construction,
                        usb_a, usb_c, video_slots, touch_screen, kb_light, screen_type,
                        diagonal, resolution, refresh_rate, cpu, p_cores, e_cores,
                        ram_size, ram_slots, gpu_inserted, gpu_discrete, ssd_size,
                        ssd_slots, battery_time, weight, score
                    ) VALUES (
                        %(id)s, %(name)s, %(model)s, %(price)s, %(os_name)s, %(is_gaming)s, %(color)s, %(material)s,
                        %(construction)s, %(usb_a)s, %(usb_c)s, %(video_slots)s,
                        %(touch_screen)s, %(kb_light)s, %(screen_type)s, %(diagonal)s,
                        %(resolution)s, %(refresh_rate)s, %(cpu)s, %(p_cores)s,
                        %(e_cores)s, %(ram_size)s, %(ram_slots)s, %(gpu_inserted)s,
                        %(gpu_discrete)s, %(ssd_size)s, %(ssd_slots)s,
                        %(battery_time)s, %(weight)s, %(score)s
                    )
                    ON CONFLICT (id) DO UPDATE SET
                    price = EXCLUDED.price
                ''', {
                    'id': row['id'],
                    'name': row['name'],
                    'model': row['model'],
                    'price': int(row['price']) if row['price'] else None,
                    'os_name': row['os_name'],
                    'is_gaming': int(row['is_gaming']) if row['is_gaming'] else None,
                    'color': row['color'],
                    'material': row['material'],
                    'construction': row.get('construction'),
                    'usb_a': row.get('usb_a'),
                    'usb_c': row.get('usb_c'),
                    'video_slots': row.get('video_slots'),
                    'touch_screen': row.get('touch_screen'),
                    'kb_light': row.get('kb_light'),
                    'screen_type': row['screen_type'],
                    'diagonal': float(row['diagonal'].replace(',', '.')) if row['diagonal'] else None,
                    'resolution': row['resolution'],
                    'refresh_rate': int(row.get('refresh_rate')) if row.get('refresh_rate') else None,
                    'cpu': row['cpu'],
                    'p_cores': int(row['p_cores']) if row.get('p_cores') else None,
                    'e_cores': int(row['e_cores']) if row.get('e_cores') else None,
                    'ram_size': int(row['ram_size']) if row['ram_size'] else None,
                    'ram_slots': int(row['ram_slots']) if row.get('ram_slots') else None,
                    'gpu_inserted': row['gpu_inserted'],
                    'gpu_discrete': row['gpu_discrete'],
                    'ssd_size': int(row['ssd_size']) if row['ssd_size'] else None,
                    'ssd_slots': row['ssd_slots'],
                    'battery_time': float(row['battery_time'].replace(',', '.')) if row['battery_time'] else None,
                    'weight': float(row['weight'].replace(',', '.')) if row['weight'] else None,
                    'score': float(row['score'])
                })
    conn.commit()
    print("Данные успешно загружены в таблицу laptops.")

def main():
    conn = psycopg2.connect(**DB_CONFIG)

    create_table(conn)
    insert_data(conn, CSV_FILE)

    conn.close()

if __name__ == '__main__':
    main()
