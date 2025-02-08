import os

# Повний шлях до JAR-файлу
MYSQL_JAR_PATH = "/home/hellcat/Projects/goit-de-fp/Streaming_Pipeline/libs/mysql-connector-j-8.0.32.jar"

# Параметри підключення до MySQL (основна база)
MYSQL_HOST = "217.61.57.46"
MYSQL_PORT = "3306"
MYSQL_USER = "neo_data_admin"
MYSQL_PASSWORD = "Proyahaxuqithab9oplp"
MYSQL_DATABASE = "olympic_dataset"

# Таблиці у MySQL
MYSQL_TABLE_ATHLETE_BIO = "athlete_bio"
MYSQL_TABLE_ATHLETE_EVENT_RESULTS = "athlete_event_results"

# Параметри підключення до MySQL (агреговані дані)
MYSQL_AGG_DATABASE = "dag_palchyk1984"
MYSQL_AGG_TABLE = "aggregated_athlete_results"

# Параметри підключення до Kafka
KAFKA_BROKER = "77.81.230.104:9092"
KAFKA_USER = "admin"
KAFKA_PASSWORD = "VawEzo1ikLtrA8Ug8THa"

# Топіки Kafka
KAFKA_TOPIC_EVENTS = "palchyk1984_athlete_event_results"
KAFKA_TOPIC_AGGREGATED = "palchyk1984_aggregated_athlete_results"
