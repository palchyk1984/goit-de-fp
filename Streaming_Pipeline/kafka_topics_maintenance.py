# Файл: kafka_topics_maintenance.py

from confluent_kafka.admin import AdminClient, NewTopic

# Підставте власні реквізити Kafka
BROKER = "77.81.230.104:9092"
USERNAME = "admin"
PASSWORD = "VawEzo1ikLtrA8Ug8THa"

# Топіки, які треба видалити та створити заново
TOPICS = [
    "palchyk1984_athlete_event_results",
    "palchyk1984_aggregated_athlete_results"
]

def reset_kafka_topics(topics=TOPICS):
    admin_conf = {
        "bootstrap.servers": BROKER,
        "security.protocol": "SASL_PLAINTEXT",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": USERNAME,
        "sasl.password": PASSWORD
    }

    admin_client = AdminClient(admin_conf)

    # Видаляємо топіки
    delete_futures = admin_client.delete_topics(topics, operation_timeout=30)
    for t, f in delete_futures.items():
        try:
            f.result()
            print(f"✅ Топік {t} успішно видалено.")
        except Exception as e:
            print(f"⚠️ Помилка при видаленні топіка {t}: {e}")

    # Створюємо топіки
    new_topics = [NewTopic(topic=t, num_partitions=1, replication_factor=1) for t in topics]
    create_futures = admin_client.create_topics(new_topics)
    for t, f in create_futures.items():
        try:
            f.result()
            print(f"✅ Топік {t} успішно створено.")
        except Exception as e:
            print(f"⚠️ Помилка при створенні топіка {t}: {e}")

if __name__ == "__main__":
    reset_kafka_topics()
