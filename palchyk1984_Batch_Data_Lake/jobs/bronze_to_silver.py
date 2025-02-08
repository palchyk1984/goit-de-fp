"""
Модуль bronze_to_silver
-----------------------
Скрипт, що виконує:
1) Читання даних із bronze/<table_name>
2) Очищення тексту від зайвих символів (REGEX)
3) Видалення дублікатів
4) Запис у silver/<table_name>
"""

import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType

def clean_text(text):
    """
    Очищує рядок від символів, які не входять у: a-z, A-Z, 0-9, ',', '.', '\\', '"' і т.д.
    Використовує регулярний вираз для видалення зайвих символів.
    """
    return re.sub(r'[^a-zA-Z0-9,.\\\"\' ]', '', str(text))

def main(table_name):
    """
    Основна функція:
      - створює SparkSession
      - читає parquet із bronze/table_name
      - застосовує UDF для очищення текстових полів
      - видаляє дублікатні рядки
      - записує результат у silver/table_name
    """
    spark = SparkSession.builder \
        .appName(f"bronze_to_silver_{table_name}") \
        .getOrCreate()

    # 1. Читаємо parquet з bronze/<table_name>
    bronze_path = f"bronze/{table_name}"
    df = spark.read.parquet(bronze_path)

    # 2. Очищення тексту у текстових колонках (UDF clean_text)
    clean_text_udf = udf(clean_text, StringType())
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(field.name, clean_text_udf(col(field.name)))

    # 3. Видаляємо дублі (всі колонки враховуються)
    df = df.dropDuplicates()

    # >>> Показуємо перші 10 рядків перед записом (для відлагодження)
    df.show(10, truncate=False)

    # 4. Запис у silver/<table_name>
    silver_path = f"silver/{table_name}"
    df.write.mode("overwrite").parquet(silver_path)
    print(f"Written cleaned & deduplicated data to {silver_path}")

    spark.stop()

if __name__ == "__main__":
    # За замовчуванням виконуємо для двох таблиць, але можна розширити
    main("athlete_bio")
    main("athlete_event_results")
