#!/bin/bash
set -e

# Установка зависимостей для Python-скрипта
pip install psycopg2-binary

# Запуск вашего Python-скрипта
python /docker-entrypoint-initdb.d/init_db.py