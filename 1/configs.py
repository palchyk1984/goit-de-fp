MYSQL_CONFIG = {
    "url": "jdbc:mysql://217.61.57.46:3306/olympic_dataset",
    "user": "neo_data_admin",
    "password": "Proyahaxuqithab9oplp",
    "driver": "com.mysql.cj.jdbc.Driver",
    "table_athlete_bio": "athlete_bio",
    "table_athlete_event_results": "athlete_event_results",
}

KAFKA_CONFIG = {
    "bootstrap_servers": "localhost:9092",
    "topics": ["athlete_event_results", "aggregated_athlete_results"],
    "partitions": 3,
    "replication_factor": 1
}

SPARK_CONFIG = {
    "mysql_connector": "/opt/spark/jars/mysql-connector-java-8.0.30.jar",
    "kafka_connector": "/opt/spark/jars/spark-sql-kafka-0-10_2.12-3.4.4.jar"
}
