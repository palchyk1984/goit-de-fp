from pyspark.sql import SparkSession
import configs
import functions

# Логування
print(f"🔹 Використовуємо JAR-файл: {configs.MYSQL_JAR_PATH}")
print(f"🔹 Підключення до MySQL: {configs.MYSQL_HOST}:{configs.MYSQL_PORT}/{configs.MYSQL_DATABASE}")

# Ініціалізуємо Spark
spark = SparkSession.builder \
    .appName("AthleteDataPipeline") \
    .config("spark.jars", configs.MYSQL_JAR_PATH) \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4") \
    .getOrCreate()

# 1️⃣ Читаємо `athlete_bio` з MySQL
print("📥 Читаємо `athlete_bio` з MySQL...")
bio_df = functions.read_athlete_bio(spark)
print(f"✅ Завантажено {bio_df.count()} записів.")

# 2️⃣ Фільтруємо некоректні дані
print("🔍 Фільтруємо некоректні записи (height, weight)...")
filtered_bio_df = functions.filter_invalid_data(bio_df)
print(f"✅ Залишилось {filtered_bio_df.count()} записів після фільтрації.")

# 3️⃣ Читаємо `athlete_event_results` з MySQL
print("📥 Читаємо `athlete_event_results` з MySQL...")
event_results_df = functions.read_athlete_event_results(spark)
print(f"✅ Завантажено {event_results_df.count()} записів.")

# 4️⃣ Записуємо `athlete_event_results` у Kafka
print(f"📤 Записуємо `athlete_event_results` у Kafka (топік {configs.KAFKA_TOPIC_EVENTS})...")
functions.write_to_kafka(event_results_df, configs.KAFKA_TOPIC_EVENTS)

# 5️⃣ Читаємо `athlete_event_results` з Kafka
print(f"📥 Читаємо `athlete_event_results` з Kafka (топік {configs.KAFKA_TOPIC_EVENTS})...")
kafka_df = functions.read_from_kafka(spark, configs.KAFKA_TOPIC_EVENTS)

# ✅ Додана перевірка сирих даних перед трансформацією
print("🔍 Перевіряємо сирі дані з Kafka перед трансформацією...")
kafka_df.show(5, truncate=False)

# 6️⃣ Конвертуємо JSON → DataFrame
print("🔄 Перетворюємо JSON із Kafka у DataFrame...")
event_df = functions.transform_kafka_data(kafka_df)

# ✅ Перевірка 3: Вивід 5 записів з `event_df`
print("🔍 Перевіряємо, чи Kafka-топік правильно передає JSON у Spark...")
event_df.show(5, truncate=False)

# 7️⃣ Об'єднуємо `athlete_event_results` із `athlete_bio`
print("🔗 Об'єднуємо `athlete_event_results` із `athlete_bio` за `athlete_id`...")
joined_df = functions.join_data(event_df, filtered_bio_df)

# 8️⃣ Агрегуємо середні значення
print("📊 Обчислюємо середній зріст і вагу по `sport`, `medal`, `sex`, `country_noc`...")
aggregated_df = functions.aggregate_data(joined_df)

# 9️⃣ Записуємо агреговані дані у Kafka
print(f"📤 Записуємо агреговані дані у Kafka (топік {configs.KAFKA_TOPIC_AGGREGATED})...")
functions.write_to_kafka(aggregated_df, configs.KAFKA_TOPIC_AGGREGATED)

# 🔟 Записуємо агреговані дані у MySQL (`dag_palchyk1984.aggregated_athlete_results`)
print(f"📥 Записуємо агреговані дані у MySQL таблицю {configs.MYSQL_AGG_TABLE}...")
functions.write_to_mysql(aggregated_df)

# Завершуємо роботу Spark
print("✅ Завершення роботи.")
spark.stop()
