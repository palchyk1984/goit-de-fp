from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import configs

def read_athlete_bio(spark):
    return spark.read \
        .format("jdbc") \
        .option("url", f"jdbc:mysql://{configs.MYSQL_HOST}:{configs.MYSQL_PORT}/{configs.MYSQL_DATABASE}") \
        .option("dbtable", configs.MYSQL_TABLE_ATHLETE_BIO) \
        .option("user", configs.MYSQL_USER) \
        .option("password", configs.MYSQL_PASSWORD) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .load()

def filter_invalid_data(df):
    return df.filter(F.col("height").isNotNull() & F.col("weight").isNotNull())

def read_athlete_event_results(spark):
    return spark.read \
        .format("jdbc") \
        .option("url", f"jdbc:mysql://{configs.MYSQL_HOST}:{configs.MYSQL_PORT}/{configs.MYSQL_DATABASE}") \
        .option("dbtable", configs.MYSQL_TABLE_ATHLETE_EVENT_RESULTS) \
        .option("user", configs.MYSQL_USER) \
        .option("password", configs.MYSQL_PASSWORD) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .load()

def write_to_kafka(df, topic):
    df.selectExpr("to_json(struct(*)) AS value") \
        .write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", configs.KAFKA_BROKER) \
        .option("kafka.security.protocol", "SASL_PLAINTEXT") \
        .option("kafka.sasl.mechanism", "PLAIN") \
        .option(
            "kafka.sasl.jaas.config",
            f'org.apache.kafka.common.security.plain.PlainLoginModule required '
            f'username="{configs.KAFKA_USER}" '
            f'password="{configs.KAFKA_PASSWORD}";'
        ) \
        .option("topic", topic) \
        .save()

    print(f"✅ Дані записані у Kafka-топік {topic}")

def read_from_kafka(spark, topic):
    df = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", configs.KAFKA_BROKER) \
        .option("kafka.security.protocol", "SASL_PLAINTEXT") \
        .option("kafka.sasl.mechanism", "PLAIN") \
        .option(
            "kafka.sasl.jaas.config",
            f'org.apache.kafka.common.security.plain.PlainLoginModule required '
            f'username="{configs.KAFKA_USER}" '
            f'password="{configs.KAFKA_PASSWORD}";'
        ) \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .load()

    return df.selectExpr("CAST(value AS STRING)")

def transform_kafka_data(df):
    schema = StructType([
        StructField("edition", StringType(), True),
        StructField("edition_id", StringType(), True),
        StructField("country_noc", StringType(), True),
        StructField("sport", StringType(), True),
        StructField("event", StringType(), True),
        StructField("result_id", StringType(), True),
        StructField("athlete", StringType(), True),
        StructField("athlete_id", StringType(), True),
        StructField("pos", StringType(), True),
        StructField("medal", StringType(), True),
        StructField("isTeamSport", StringType(), True)
    ])

    df = df.select(F.from_json(F.col("value"), schema).alias("data")).select("data.*")

    df = df.withColumn("medal", F.when(F.col("medal") == "nan", None).otherwise(F.col("medal")))

    return df

def join_data(event_df, bio_df):
    return event_df.join(
        bio_df,
        "athlete_id",
        "inner"
    ).select(
        event_df["athlete_id"],
        event_df["sport"],
        event_df["medal"],
        bio_df["sex"],
        event_df["country_noc"],
        bio_df["height"],
        bio_df["weight"]
    )

def aggregate_data(df):
    agg = df.groupBy("sport", "medal", "sex", "country_noc") \
        .agg(
            F.avg("height").alias("avg_height"),
            F.avg("weight").alias("avg_weight")
        ) \
        .withColumn("timestamp", F.current_timestamp())

    # Замінюємо NaN на None перед записом
    agg = agg \
        .withColumn("avg_height", F.when(F.isnan(F.col("avg_height")), None).otherwise(F.col("avg_height"))) \
        .withColumn("avg_weight", F.when(F.isnan(F.col("avg_weight")), None).otherwise(F.col("avg_weight")))

    return agg

def write_to_mysql(df):
    df.write \
        .format("jdbc") \
        .option("url", f"jdbc:mysql://{configs.MYSQL_HOST}:{configs.MYSQL_PORT}/{configs.MYSQL_AGG_DATABASE}") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("user", configs.MYSQL_USER) \
        .option("password", configs.MYSQL_PASSWORD) \
        .option("dbtable", configs.MYSQL_AGG_TABLE) \
        .mode("append") \
        .save()

    print(f"✅ Дані записані у MySQL таблицю {configs.MYSQL_AGG_TABLE}")
