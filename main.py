import shutil
import os
import asyncio
import logging
from typing import List, Dict, Any

from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
from query_builder import build_sql_query
import pandas as pd
import psycopg2
from dotenv import load_dotenv

from langchain_huggingface import HuggingFaceEmbeddings 
from langchain_chroma import Chroma 
from langchain.schema import Document as LangchainDocument # Переименовываем, чтобы не конфликтовать с Pydantic моделью
from langchain.text_splitter import RecursiveCharacterTextSplitter #
from langchain.chains import RetrievalQA 
from langchain_ollama import OllamaLLM 
from chromadb.config import Settings

# --- Настройки логирования ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Конфигурация (можно вынести в отдельный config.py или .env файл) ---
# Настройки подключения к базе данных
load_dotenv()

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST"),
    "port": os.getenv("POSTGRES_PORT"),
    "dbname": "laptops_db",
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD")
}

# Пути (убедитесь, что они корректны относительно места запуска FastAPI)
OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://localhost:11434")
CHROMA_DIR_BASE = "chroma" # Базовая директория для ChromaDB
MD_DIR = os.path.abspath("laptop_markdown_files") # Абсолютный путь для надежности
COLLECTION_NAME = "laptops_collection"

# --- Глобальные объекты (загружаются один раз при старте) ---
try:
    LLM = OllamaLLM(
        model='gemma3:1b',
        base_url=OLLAMA_HOST,
        temperature=0.2,
        verbose=False, 
    )
    logger.info(f"Модель LlamaCpp успешно загружена из Ollama")
except Exception as e:
    logger.error(f"Ошибка при загрузке модели LlamaCpp из Ollama: {e}", exc_info=True)
    LLM = None

try:
    EMBEDDING_MODEL = HuggingFaceEmbeddings(model_name="intfloat/multilingual-e5-base")
    logger.info("Модель эмбеддингов HuggingFace успешно загружена.")
except Exception as e:
    logger.error(f"Ошибка при загрузке модели эмбеддингов: {e}", exc_info=True)
    EMBEDDING_MODEL = None

# Проверяем наличие директории с markdown файлами
if not os.path.isdir(MD_DIR):
    logger.error(f"Критическая ошибка: Директория с markdown файлами не найдена: {MD_DIR}")
    # Это также критично для работы функции choose_best_laptop

app = FastAPI(
    title="API подбора ноутбуков",
    description="API для анализа отзывов ноутбуков с использованием RAG и LLM.",
    version="1.0.0"
)

# --- Pydantic модели для валидации запросов и ответов ---
class UserAnswers(BaseModel):
    budget: int
    osName: str
    screenSize: List[float]
    usageScenario: str
    caseMaterial: str
    additionalFeatures: List[str]
    batteryTime: str
    weight: int

class LaptopChoiceRequest(BaseModel):
    top10_list: List[str]
    use_case: str

class DocumentMetadata(BaseModel):
    model: str | None = None # Поле model в метаданных

class DocumentResponse(BaseModel):
    page_content: str
    metadata: DocumentMetadata # Используем вложенную модель для метаданных

class LaptopChoiceResponse(BaseModel):
    result: str
    source_documents: List[DocumentResponse] | None = None # Могут отсутствовать при ошибке
    error: str | None = None


# --- Вспомогательные функции (из вашего app_rag.py) ---
def load_markdown_files(model_names: List[str]) -> List[LangchainDocument]:
    docs = []
    if not os.path.isdir(MD_DIR):
        logger.error(f"Директория с markdown файлами не найдена при вызове load_markdown_files: {MD_DIR}")
        return []
    for name in model_names:
        filename = os.path.join(MD_DIR, f"{name}.md")
        if not os.path.exists(filename):
            logger.warning(f"[!] Файл не найден: {filename}")
            continue
        try:
            with open(filename, "r", encoding="utf-8") as f:
                text = f.read()
            docs.append(LangchainDocument(page_content=text, metadata={"model": name}))
        except Exception as e:
            logger.error(f"Ошибка при чтении файла {filename}: {e}", exc_info=True)
    return docs

async def _build_chroma_and_retriever(chunks: List[LangchainDocument], request_id: str):
    """
    Асинхронная обертка для блокирующих операций ChromaDB.
    Создает уникальную директорию для каждого запроса.
    """
    if EMBEDDING_MODEL is None:
        raise RuntimeError("Модель эмбеддингов не загружена.")

    # Создаем уникальную директорию для ChromaDB для этого запроса, чтобы избежать конфликтов
    chroma_instance_dir = os.path.join(CHROMA_DIR_BASE, request_id)
    os.makedirs(chroma_instance_dir, exist_ok=True)

    logger.info(f"[{request_id}] Создание ChromaDB в {chroma_instance_dir} для {len(chunks)} чанков...")
    
    try:
        
        chroma_db = await asyncio.to_thread(
            Chroma.from_documents,
            documents=chunks,
            embedding=EMBEDDING_MODEL,
            collection_name=COLLECTION_NAME,
            persist_directory=chroma_instance_dir 
        )
        retriever = chroma_db.as_retriever(search_kwargs={"k": min(20, len(chunks))}) 
        logger.info(f"[{request_id}] ChromaDB и retriever успешно созданы.")
        return retriever, chroma_instance_dir
    except Exception as e:
        logger.error(f"[{request_id}] Ошибка при создании ChromaDB: {e}", exc_info=True)
        return {"error": f"Ошибка при создании ChromaDB: {str(e)}"} 


# --- Основная логика RAG (адаптированная для FastAPI) ---
async def choose_best_laptop_logic(
    top10_list: List[str],
    use_case: str,
    request_id: str, # Для логирования и уникальных директорий Chroma
    background_tasks: BackgroundTasks
) -> Dict[str, Any]:
    if LLM is None:
        logger.error(f"[{request_id}] Модель LLM не загружена. Невозможно выполнить запрос.")
        return {"error": "Модель LLM не сконфигурирована на сервере."}
    if EMBEDDING_MODEL is None:
        logger.error(f"[{request_id}] Модель эмбеддингов не загружена. Невозможно выполнить запрос.")
        return {"error": "Модель эмбеддингов не сконфигурирована на сервере."}
    if not os.path.isdir(MD_DIR):
        logger.error(f"[{request_id}] Директория с markdown файлами не найдена: {MD_DIR}")
        return {"error": "Отсутствуют файлы с описаниями ноутбуков на сервере."}


    logger.info(f"[{request_id}] Запрос на выбор лучшего ноутбука для моделей: {top10_list}, сценарии: {use_case}")

    # 1. Загрузка markdown-файлов (блокирующая операция, но быстрая)
    documents = await asyncio.to_thread(load_markdown_files, top10_list)
    if not documents:
        logger.warning(f"[{request_id}] Отзывы не найдены для моделей: {top10_list}")
        return {"error": "Отзывы не найдены для выбранных моделей."}

    # 2. Разбиение текста
    splitter = RecursiveCharacterTextSplitter(chunk_size=500, chunk_overlap=100)
    chunks = splitter.split_documents(documents)
    if not chunks:
        logger.warning(f"[{request_id}] Разбиение текста не дало результатов для {len(documents)} документов.")
        return {"error": "Не удалось обработать тексты отзывов."}
    logger.info(f"[{request_id}] Текст разбит на {len(chunks)} чанков.")

    chroma_instance_dir = None
    try:
        # 3. Создание ChromaDB и retriever (асинхронно с блокирующими частями в потоке)
        retriever, chroma_instance_dir = await _build_chroma_and_retriever(chunks, request_id)    

        # 4. Построение запроса к LLM
        model_names_str = ", ".join(top10_list)
        use_cases_str = ", ".join(use_case)
        query = (
            f"Вот отзывы и описания нескольких ноутбуков. "
            f"Сравни ТОЛЬКО следующие модели: {model_names_str}. "
            f"Выбери ТРИ лучших с точки зрения следующих сценариев использования: {use_cases_str}. "
            f"Перечисли списком ТОЛЬКО наименования моделей, без дальнейших объяснений, separator = \\n. "
            f"Отвечай ТОЛЬКО на русском языке. Не используй английский."
        )
        logger.debug(f"[{request_id}] Сформирован запрос к LLM: {query}")

        # 5. Исполнение QA chain
        qa_chain = RetrievalQA.from_chain_type(
            llm=LLM,
            retriever=retriever,
            return_source_documents=True
        )

        logger.info(f"[{request_id}] Выполнение QA chain...")
        # qa_chain.ainvoke - асинхронный метод
        response = await qa_chain.ainvoke({"query": query}) # Передаем query в словаре
        logger.info(f"[{request_id}] QA chain успешно выполнен. Результат: {response.get('result')}")

        # 6. Формирование ответа
        # Преобразование LangchainDocument в DocumentResponse для Pydantic модели
        source_doc_responses = []
        if response.get("source_documents"):
            for doc in response["source_documents"]:
                source_doc_responses.append(
                    DocumentResponse(
                        page_content=doc.page_content,
                        metadata=DocumentMetadata(model=doc.metadata.get("model"))
                    )
                )
        
        return {
            "result": response.get("result", "Нет результата от LLM."),
            "source_documents": source_doc_responses,
            "verbose": {
                "full_response": response,
                "model": response.get("model"),
                "tokens_used": response.get("eval_count"),
                "timing_info": response.get("timing")
            }
        }

    except Exception as e:
        logger.error(f"[{request_id}] Непредвиденная ошибка в choose_best_laptop_logic: {e}", exc_info=True)
        return {"error": f"Внутренняя ошибка сервера при обработке запроса: {str(e)}"}


# --------------------- FastAPI эндпоинт ----------------------------



def submit_answers(answers: UserAnswers):
    sql, params = build_sql_query(answers.model_dump())
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                if cur.description is None:
                    raise HTTPException(status_code=500, detail="Запрос выполнен, но не вернул результат. Возможно, ошибка в SQL.")
                if cur.description:
                    columns = [desc[0] for desc in cur.description]
                    rows = cur.fetchall()
                    df = pd.DataFrame(rows, columns=columns)
                else:
                    df = pd.DataFrame()

        top10_names = df.sort_values(by=["score", "price"], ascending=[False, True])["name"].head(10).tolist()
        return LaptopChoiceRequest(
            top10_list=top10_names,
            use_case=answers.usageScenario
        )
    
    #top10_names, use_cases
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Внутренняя ошибка сервера: {str(e)}")
    

@app.post("/submit", response_model=LaptopChoiceResponse)
async def api_choose_laptop(answers: UserAnswers, background_tasks: BackgroundTasks):
    """
    Принимает список из топ-10 моделей ноутбуков и сценарии использования.
    Возвращает 3 лучших модели на основе анализа отзывов с помощью RAG и LLM.
    """
    request = submit_answers(answers)
    # Генерируем уникальный ID для запроса (для логов и временных файлов)
    request_id = str(os.urandom(4).hex())
    logger.info(f"[{request_id}] Получен запрос на /choose_laptop: top10_list={request.top10_list}, use_cases={request.use_case}")

    if not request.top10_list:
        raise HTTPException(status_code=400, detail="Параметр 'top10_list' не может быть пустым.")
    if not request.use_case:
        raise HTTPException(status_code=400, detail="Параметр 'use_case' не может быть пустым.")

    try:
        result_data = await choose_best_laptop_logic(
            request.top10_list,
            request.use_case,
            request_id,
            background_tasks
        )

        if "error" in result_data:
            # Если ошибка произошла внутри логики, но не была HTTPException
            logger.error(f"[{request_id}] Ошибка при обработке: {result_data['error']}")
            # Возвращаем 200 OK, но с полем error в теле JSON, как определено в LaptopChoiceResponse
            return LaptopChoiceResponse(result="Ошибка обработки", error=result_data['error'])

        return LaptopChoiceResponse(**result_data)

    except HTTPException:
        raise # Перебрасываем HTTPException, чтобы FastAPI обработал их корректно
    except Exception as e:
        logger.error(f"[{request_id}] Непредвиденная ошибка в эндпоинте /choose_laptop: {e}", exc_info=True)
        # Это будет 500 ошибка, так как она не обработана как ошибка бизнес-логики
        raise HTTPException(status_code=500, detail=f"Внутренняя ошибка сервера: {str(e)}")

@app.get("/health")
async def health_check():
    """Проверка работоспособности API."""
    llm_status = "OK" if LLM else "Ошибка загрузки"
    embed_status = "OK" if EMBEDDING_MODEL else "Ошибка загрузки"
    md_dir_status = "OK" if os.path.isdir(MD_DIR) else "Не найдена"

    return {
        "status": "healthy",
        "llm_model": llm_status,
        "embedding_model": embed_status,
        "markdown_directory": md_dir_status,
    }

# --- Запуск FastAPI (если файл запускается напрямую) ---
if __name__ == "__main__":
    # Убедитесь, что директории существуют или создайте их
    if not os.path.exists(CHROMA_DIR_BASE):
        os.makedirs(CHROMA_DIR_BASE, exist_ok=True)
    if not os.path.exists(MD_DIR):
        logger.error(f"Директория с markdown файлами {MD_DIR} не существует. Пожалуйста, создайте ее и поместите туда файлы.")
        # exit(1) # Можно раскомментировать для прекращения работы, если директория критична

    logger.info(f"MD_DIR: {MD_DIR}")
    logger.info(f"CHROMA_DIR_BASE: {CHROMA_DIR_BASE}")

    import uvicorn
    # Для доступа из эмулятора Android или другого устройства в той же сети,
    # используйте host="0.0.0.0".
    # Убедитесь, что ваш файрвол разрешает подключения на этот порт.
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")