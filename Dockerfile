FROM apache/airflow:2.8.1-python3.10
RUN pip install --no-cache-dir langchain-ollama pandas openpyxl
