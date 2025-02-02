from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from kafka import KafkaAdminClient
from kafka.admin import NewTopic
import subprocess
import configs


def create_spark_session():
    """Создает Spark сессию с необходимыми библиотеками."""
    try:
        spark = SparkSession.builder \
            .appName("OlympicAthleteProcessing") \
            .config("spark.jars", f"{configs.SPARK_CONFIG['mysql_connector']},{configs.SPARK_CONFIG['kafka_connector']}") \
            .getOrCreate()
        print("✅ Spark Session created successfully")
        return spark
    except Exception as e:
        print(f"❌ Error creating Spark Session: {e}")
        exit(1)


def check_kafka_connection(config):
    """Проверяет подключение к Kafka."""
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=config["bootstrap_servers"])
        print("✅ Connected to Kafka successfully")
        return admin_client
    except Exception as e:
        print(f"❌ Error connecting to Kafka: {e}")
        exit(1)


def ensure_kafka_topics(admin_client, config):
    """Проверяет, что необходимые топики существуют, и создает их, если отсутствуют."""
    existing_topics = admin_client.list_topics()
    
    for topic in config["topics"]:
        if topic not in existing_topics:
            print(f"⚠ Топик '{topic}' не существует. Создаем...")
            try:
                admin_client.create_topics([
                    NewTopic(
                        name=topic,
                        num_partitions=config["partitions"],
                        replication_factor=config["replication_factor"]
                    )
                ])
                print(f"✅ Топик '{topic}' успешно создан")
            except Exception as e:
                print(f"❌ Ошибка при создании топика '{topic}': {e}")
                # Если создание через API не сработало, создаем топик через Docker
                subprocess.run([
                    "docker", "exec", "-it", "kafka", "kafka-topics",
                    "--create", "--topic", topic,
                    "--bootstrap-server", config["bootstrap_servers"],
                    "--partitions", str(config["partitions"]),
                    "--replication-factor", str(config["replication_factor"])
                ])
        else:
            print(f"✅ Топик '{topic}' уже существует")


def read_mysql_table(spark, config, table_name):
    """Читает данные из таблицы MySQL."""
    try:
        df = spark.read \
            .format("jdbc") \
            .option("url", config["url"]) \
            .option("dbtable", table_name) \
            .option("user", config["user"]) \
            .option("password", config["password"]) \
            .option("driver", config["driver"]) \
            .load()
        print(f"✅ Успешно прочитано {df.count()} записей из MySQL таблицы {table_name}")
        return df
    except Exception as e:
        print(f"❌ Ошибка при чтении MySQL таблицы {table_name}: {e}")
        exit(1)


def write_to_kafka(df, topic, kafka_config):
    """Записывает данные в Kafka-топик."""
    try:
        df.selectExpr("to_json(struct(*)) AS value") \
            .write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"]) \
            .option("topic", topic) \
            .save()
        
        print(f"✅ Успешно записано {df.count()} записей в Kafka топик '{topic}'")
        
        # Для проверки читаем сообщения из топика через Docker
        subprocess.run([
            "docker", "exec", "-it", "kafka", "kafka-console-consumer",
            "--bootstrap-server", kafka_config["bootstrap_servers"],
            "--topic", topic,
            "--from-beginning"
        ])
    except Exception as e:
        print(f"❌ Ошибка при записи в Kafka топик '{topic}': {e}")
        exit(1)
