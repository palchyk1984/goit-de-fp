from kafka import KafkaConsumer
import configs

try:
    # Створюємо Kafka consumer
    consumer = KafkaConsumer(
        configs.KAFKA_TOPIC_EVENTS,
        bootstrap_servers=configs.KAFKA_BROKER,
        security_protocol="SASL_PLAINTEXT",
        sasl_mechanism="PLAIN",
        sasl_plain_username=configs.KAFKA_USER,
        sasl_plain_password=configs.KAFKA_PASSWORD,
        auto_offset_reset="earliest"
    )

    print(f"✅ Читаємо дані з топіка {configs.KAFKA_TOPIC_EVENTS}...\n")

    for message in consumer:
        print(f"📥 Отримано: {message.value.decode('utf-8')}")
        break  # Виходимо після першого повідомлення

except Exception as e:
    print(f"❌ Помилка підключення до Kafka: {e}")
