from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,FloatType
from pyspark.sql.functions import avg,from_json,to_timestamp,col,round,sum,trim,lower,countDistinct,unix_timestamp,count,when,lit
from builtins import round as builtin_round
from kafka_consumer import get_data_from_kafka

orderSchema = StructType([
    StructField("Id",IntegerType(),nullable=True),
    StructField("order_status",StringType(),nullable=True),
    StructField("order_products_value",FloatType(),nullable=True),
    StructField("order_freight_value",FloatType(),nullable=True),
    StructField("order_items_qty",IntegerType(),nullable=True),
    StructField("order_purchase_timestamp",StringType(),nullable=True),
    StructField("order_aproved_at",StringType(),nullable=True),
    StructField("order_delivered_customer_date",StringType(),nullable=True),
    StructField("customer_city",StringType(),nullable=True),
    StructField("customer_state",StringType(),nullable=True),
    StructField("customer_zip_code_prefix",IntegerType(),nullable=True),
    StructField("review_score",IntegerType(),nullable=True),
])

scala_version = '2.12'
spark_version = '3.5.0'

packages = [
    f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
    'org.apache.kafka:kafka-clients:3.4.1'
]

spark = (SparkSession
            .builder.master("local[*]")
            .appName("assignment")
            .config("spark.jars.packages", ",".join(packages))
            .enableHiveSupport()
            .getOrCreate()
            )

## to read directly from kafka to spark dataframe
# df = spark \
#   .read \
#   .format("kafka") \
#   .option("kafka.bootstrap.servers", "192.168.56.131:9092") \
#   .option("subscribe", "assignment") \
#   .option("startingOffsets", "earliest") \
#   .load()


# deserDF = df.select(from_json(col("value").cast("string"), orderSchema).alias("value")).select("value.*")

## to read from kafka to pandas dataframe and then convert to spark dataframe
deserDF = spark.createDataFrame(get_data_from_kafka())

deserDF.show()

parsedDF = deserDF  \
      .withColumn("order_delivered_customer_date",to_timestamp(col("order_delivered_customer_date"))) \
      .withColumn("order_approved_at",to_timestamp(col("order_aproved_at"))) \
      .withColumn("order_purchase_timestamp",to_timestamp(col("order_purchase_timestamp")))\
      .withColumn("order_status",lower(trim(col("order_status")))) \
      .withColumn("customer_city",lower(trim(col("customer_city")))) \
      .withColumn("customer_state",lower(trim(col("customer_state"))))

## Identify the null values
nullValuedDF  = parsedDF.select([count(when(col(c).contains('None') | \
                            col(c).contains('NULL') | \
                            (trim(col(c)) == '' ) | \
                            col(c).isNull(),c
                           )).alias(c)
                    for c in parsedDF.columns])

## As null valued rows are less, so i am going to ignore those rows in futher computation.
nullValuedDF.select("order_delivered_customer_date").show()

## drop null rows and ignore invalid records
cleanParsedDF = parsedDF.where(
    col("order_delivered_customer_date").isNotNull() 
    & (unix_timestamp(col("order_delivered_customer_date"))-unix_timestamp(col("order_approved_at")) > 0)
    & (unix_timestamp(col("order_approved_at"))-unix_timestamp(col("order_purchase_timestamp")) > 0))


## EDA
aggregatedDF = cleanParsedDF.select(
      round(avg(col("order_products_value")),2).alias("mean_order_products_value"),
      round(avg(col("order_freight_value")),2).alias("mean_order_freight_value")
)

aggregatedDF.show()

orderDistributedDF = cleanParsedDF \
                    .groupBy(col("order_status")) \
                    .count()

orderDistributedDF.show()

uniqueStatesCount = cleanParsedDF.select(countDistinct(col("customer_state")))


print(uniqueStatesCount)

maxOrderCitiesCount = cleanParsedDF \
                    .groupBy(col("customer_city")) \
                    .count() \
                    .orderBy("count",ascending=False) \
                    .limit(5)

maxOrderCitiesCount.show()

cleanParsedDF.createOrReplaceTempView("test")

spark.sql("""
    SELECT order_status,
        count(*)/(SELECT count(*) FROM test)*100 as order_quantity_percent 
    FROM test 
      GROUP BY order_status 
    HAVING order_status IN ('delivered','canceled');
""").show()


## Transforms

totalOrderPerCity = cleanParsedDF \
                    .groupBy(col("customer_city")) \
                    .agg(round(sum(col("order_products_value")),2).alias("order_cost")) \
                    .orderBy("order_cost",ascending=False)

totalOrderPerCity.show()

orderFreightOrderItemsCorr = cleanParsedDF.corr("order_freight_value","order_items_qty")

print(builtin_round(orderFreightOrderItemsCorr,2))

deliveryTimeSecondsDF = cleanParsedDF \
    .withColumn("delivery_time_in_seconds",unix_timestamp(col("order_delivered_customer_date"))-unix_timestamp(col("order_approved_at"))) \
    .withColumn("approved_time_in_seconds",unix_timestamp(col("order_approved_at"))-unix_timestamp(col("order_purchase_timestamp"))) \
    .select("review_score","customer_city","order_status","order_approved_at","order_delivered_customer_date","delivery_time_in_seconds","approved_time_in_seconds")



averageDeliveryTimeAndApprovedTimeDF = deliveryTimeSecondsDF \
.where(col("order_status") == "delivered") \
.select(
    round((avg("delivery_time_in_seconds")/(3600*24)),1).alias("average_delivery_time (days)"),
    round((avg("approved_time_in_seconds")/(3600*24)),1).alias("average_approved_time (days)")
)

averageDeliveryTimeAndApprovedTimeDF.show()

averageReviewScoreDF = deliveryTimeSecondsDF \
.where(col("order_status") == "delivered") \
.select(
    round((avg("review_score")),1).alias("average_review_score")
)

averageReviewScoreDF.show()

fastestAndSlowestDeliveryTime = deliveryTimeSecondsDF \
.where(col("order_status") == "delivered") \
.select(col("customer_city"),col("delivery_time_in_seconds")) \
.orderBy(col("delivery_time_in_seconds"))

fastestAndSlowestDeliveryTime.limit(5)
fastestAndSlowestDeliveryTime.tail(5)

deliveryTimeReviewScoreCorr = deliveryTimeSecondsDF.corr("delivery_time_in_seconds","review_score")

print(builtin_round(deliveryTimeReviewScoreCorr,2))

deliveryTimeSecondsDF.write.saveAsTable("assignment_transform_table",mode="overwrite")
cleanParsedDF.write.saveAsTable("assignment_cleaned_table",mode="overwrite")

spark.sql("SELECT * from assignment_transform_table").show()
spark.sql("SELECT * from assignment_cleaned_table").show()