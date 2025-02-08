from pyspark.sql import SparkSession
import configs

# Ініціалізуємо SparkSession
spark = SparkSession.builder \
    .appName("MySQL_Spark_Test") \
    .config("spark.jars", configs.MYSQL_JAR_PATH) \
    .getOrCreate()

try:
    # Читаємо дані з таблиці athlete_event_results
    df = spark.read \
        .format("jdbc") \
        .option("url", f"jdbc:mysql://{configs.MYSQL_HOST}:{configs.MYSQL_PORT}/{configs.MYSQL_DATABASE}") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("user", configs.MYSQL_USER) \
        .option("password", configs.MYSQL_PASSWORD) \
        .option("dbtable", configs.MYSQL_TABLE_ATHLETE_EVENT_RESULTS) \
        .load()

    # Виводимо кількість рядків
    print(f"✅ Зчитано {df.count()} записів з таблиці {configs.MYSQL_TABLE_ATHLETE_EVENT_RESULTS}.")

    # Виводимо 5 перших рядків
    df.show(5)

except Exception as e:
    print(f"❌ Помилка підключення до MySQL: {str(e)}")

finally:
    spark.stop()
