import functions as fn
import configs

def main():
    # Создаем Spark-сессию с подключенными библиотеками
    spark = fn.create_spark_session()

    # Проверяем подключение к Kafka
    admin_client = fn.check_kafka_connection(configs.KAFKA_CONFIG)

    # Убеждаемся, что необходимые топики существуют или создаем их
    fn.ensure_kafka_topics(admin_client, configs.KAFKA_CONFIG)

    # Читаем данные из MySQL (таблица athlecdte_event_results)
    df = fn.read_mysql_table(spark, configs.MYSQL_CONFIG, configs.MYSQL_CONFIG["table_athlete_event_results"])

    # Записываем данные в Kafka-топик "athlete_event_results"
    fn.write_to_kafka(df, "athlete_event_results", configs.KAFKA_CONFIG)

if __name__ == "__main__":
    main()
