"""
Модуль silver_to_gold
---------------------
Цей скрипт виконує третій етап обробки даних:
1) Зчитування очищених даних з "silver" (athlete_bio, athlete_event_results)
2) Приведення полів height/weight до float
3) Джойн таблиць за athlete_id
4) Агрегація (avg(height), avg(weight) за sport, medal, sex, country_noc)
5) Запис результату у gold/avg_stats
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, current_timestamp, col
from pyspark.sql.types import FloatType

def main():
    """
    Основна функція:
      - створює SparkSession
      - виконує читання даних із silver-слою
      - трансформує та агрегірує дані
      - записує результат у gold
    """
    spark = SparkSession.builder \
        .appName("silver_to_gold") \
        .getOrCreate()

    # 1. Зчитуємо таблиці із silver
    df_bio = spark.read.parquet("silver/athlete_bio")
    df_event = spark.read.parquet("silver/athlete_event_results")

    # 2. Приведення (height, weight) до float
    df_bio = df_bio.withColumn("weight", col("weight").cast(FloatType()))
    df_bio = df_bio.withColumn("height", col("height").cast(FloatType()))

    # 3. Join за athlete_id, залишаємо одну колонку country_noc (з df_bio)
    df_joined = (
        df_event.alias("ev")
        .join(
            df_bio.alias("bio"),
            on="athlete_id",
            how="inner"
        )
        .select(
            col("ev.athlete_id"),
            col("ev.sport"),
            col("ev.medal"),
            col("bio.height"),
            col("bio.weight"),
            col("bio.sex"),
            col("bio.country_noc"),  # беремо country_noc з 'bio'
        )
    )

    # 4. Агрегація за sport, medal, sex, country_noc
    df_agg = (
        df_joined.groupBy(
            "sport",
            "medal",
            "sex",
            "country_noc"
        )
        .agg(
            avg("weight").alias("avg_weight"),
            avg("height").alias("avg_height")
        )
        .withColumn("calculation_ts", current_timestamp())
    )

    # >>> Показуємо перші 10 рядків агрегованої таблиці (для відлагодження)
    df_agg.show(10, truncate=False)

    # 5. Запис у gold/avg_stats
    df_agg.write.mode("overwrite").parquet("gold/avg_stats")
    print("Written aggregated data to gold/avg_stats")

    spark.stop()

if __name__ == "__main__":
    main()
