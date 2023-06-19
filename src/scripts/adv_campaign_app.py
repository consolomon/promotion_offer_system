
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F  # from_json, to_json, col, lit, struct
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType


# необходимые библиотеки для интеграции Spark с Kafka и PostgreSQL
SPARK_JARS_PACKAGES = ",".join(
        [
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
            "org.postgresql:postgresql:42.4.0",
        ]
    )

# имена топиков в Kafka
TOPIC_NAME_IN = "student.topic.cohort10.consolomon-in"
TOPIC_NAME_OUT = "student.topic.cohort10.consolomon-out"

# конфигурация подключения к потоку в Kafka
KAFKA_SETTINGS = {
    'kafka.bootstrap.servers': 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091',
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"de-student\" password=\"ltcneltyn\";',
}

# конфигурация подключения к базе данных в Postgres
POSTGRES_SETTINGS = {
    'url': 'jdbc:postgresql://localhost:5432',
    'driver': 'org.postgresql.Driver',
    'user': 'jovyan',
    'password': 'jovyan'
}

# определяем схему входного сообщения для json
INPUT_KAFKA_STEAM_SCHEMA = StructType([
        StructField("restaurant_id", StringType()),
        StructField("adv_campaign_id", StringType()),
        StructField("adv_campaign_content", StringType()),
        StructField("adv_campaign_owner", StringType()),
        StructField("adv_campaign_owner_contact", StringType()),
        StructField("adv_campaign_datetime_start", LongType()),
        StructField("adv_campaign_datetime_end", LongType()),
        StructField("datetime_created", LongType()),
    ])

# метод для записи данных в 2 target: в PostgreSQL для фидбэков и в Kafka для триггеров
def foreach_batch_function(df, epoch_id):
    
    # сохраняем df в памяти, чтобы не создавать df заново перед отправкой в Kafka
    df.persist()
       
    # записываем df в PostgreSQL с полем feedback
    write_postgres_table(df)

    # создаём df для отправки в Kafka. Сериализация в json.
    # отправляем сообщения в результирующий топик Kafka без поля feedback

    write_kafka_stream(df)
    # очищаем память от df
    df.unpersist()

def spark_init() -> SparkSession:

    return SparkSession.builder \
    .appName("RestaurantSubscribeStreamingService") \
    .config("spark.sql.session.timeZone", "UTC") \
    .config("spark.jars.packages", SPARK_JARS_PACKAGES) \
    .getOrCreate()

def read_kafka_steam(spark: SparkSession) -> DataFrame:

    return spark.readStream \
    .format('kafka') \
    .options(**KAFKA_SETTINGS) \
    .option('subscribe', TOPIC_NAME_IN) \
    .load()

def read_postgres_table(spark: SparkSession) -> DataFrame:

    return spark \
          .read \
          .format('jdbc') \
          .options(**POSTGRES_SETTINGS) \
          .option('dbtable', 'de.public.subscribers_restaurants') \
          .load()

def transform_kafka_stream(df: DataFrame, schema: StructType) -> DataFrame:
    return (
        df
            .withColumn('value', F.col('value').cast(StringType()))
            .withColumn('event', F.from_json(F.col('value'), schema))
            .selectExpr('event.*')
            .withColumn('timestamp',
                        F.from_unixtime(F.col('timestamp'), "yyyy-MM-dd' 'HH:mm:ss.SSS").cast(TimestampType()))
            .dropDuplicates(["client_id","adv_campaign_id"])
            .withWatermark("timestamp", "120 minute")
    )

def write_postgres_table(df: DataFrame):

    df \
    .withColumn("feedback", F.lit(None).cast('string')) \
    .write \
    .format("jdbc") \
    .options(**POSTGRES_SETTINGS) \
    .option('dbtable', 'de.public.subscribers_feedback') \
    .save()

def write_kafka_stream(df: DataFrame, schema: StructType):
    
    df.select(
                F.to_json(
                    F.struct(
                        'restaurant_id',
                        'adv_campaign_id',
                        'adv_campaign_content',
                        'adv_campaign_owner',
                        'adv_campaign_owner_contact',
                        'adv_campaign_datetime_start'
                        'adv_campaign_end_time',
                        'client_id',
                        'datetime_created',
                        'trigger_datetime_created'
                    )
                ).alias('value')
           ) \
           .writeStream \
           .outputMode("append") \
           .format("kafka") \
           .options(**KAFKA_SETTINGS) \
           .option("topic", TOPIC_NAME_OUT) \
           .trigger(processingTime="30 seconds")

def __main__():

    # запускаем Spark-сессию
    spark = spark_init()

    # читаем новый батч из потока в kafka
    restaurant_read_stream_df = read_kafka_steam(spark)

    # определяем текущее время в UTC в миллисекундах
    current_timestamp_utc = int(round(datetime.utcnow().timestamp()))

    # десериализуем из value сообщения json и фильтруем по времени старта и окончания акции
    filtered_read_stream_df = transform_kafka_stream(restaurant_read_stream_df, INPUT_KAFKA_STEAM_SCHEMA)
    
    # вычитываем всех пользователей с подпиской на рестораны
    subscribers_restaurant_df = read_postgres_table(spark)

    # джойним данные из сообщения Kafka с пользователями подписки по restaurant_id (uuid). Добавляем время создания события.
    result_df = filtered_read_stream_df.withColumn("trigger_datetime_created", F.lit(current_timestamp_utc)) \
                                       .join(subscribers_restaurant_df, "restaurant_id", "left") \
                                       .drop(["timestamp", "id"]) \
                                       .where(
                                           "trigger_datetime_created >= adv_campaign_datetime_start and \
                                            trigger_datetime_created <= adv_campaign_datetime_end"
                                       )
                                       
    # запускаем стриминг
    result_df.writeStream \
    .foreachBatch(foreach_batch_function) \
    .start() \
    .awaitTermination()


if __name__ == "__main__":
    __main__()