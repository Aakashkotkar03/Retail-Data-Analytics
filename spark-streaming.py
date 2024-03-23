from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Create a Spark Session to process Streaming data
spark = SparkSession  \
	.builder  \
	.appName("StructuredSocketRead")  \
	.getOrCreate()
spark.sparkContext.setLogLevel('ERROR')	

# 1. UDF to get total cost of an order
def get_total_cost(order_type, items):

	total_cost = 0
	for item in items:
		total_cost = total_cost + (item['unit_price'] * item['quantity'])
	
	if order_type == "ORDER":
		return total_cost
	else:
		return total_cost * (-1)

total_cost_udf = udf(get_total_cost, FloatType())

# 2. UDF to get total items in an order
def get_total_items(items):

	total_items = 0

	for item in iter(items):
		total_items = total_items + item['quantity']

	return total_items

total_items_udf = udf(get_total_items, IntegerType())

# 3. UDF to determine if the order is an actual order
def is_order(order_type):

	if order_type == "ORDER":
		return 1
	else:
		return 0

is_order_udf = udf(is_order, IntegerType())

# 4. UDF to determine if the order is a return
def is_return(order_type):

	if order_type == "RETURN":
		return 1
	else:
		return 0

is_return_udf = udf(is_return, IntegerType())	

# Schema to read the data fromt he Kafka Producer
schema = StructType() \
	.add("type", StringType()) \
	.add("country", StringType()) \
	.add("invoice_no", LongType()) \
	.add("timestamp", TimestampType()) \
	.add("items", ArrayType(StructType() \
		.add("SKU", StringType()) \
		.add("title", StringType()) \
		.add("unit_price", FloatType()) \
		.add("quantity", IntegerType())
		))

# Read data from Kafka Producer	
orders = spark  \
	.readStream  \
	.format("kafka")  \
	.option("kafka.bootstrap.servers","18.211.252.152:9092")  \
	.option("subscribe","real-time-project")  \
	.load()

# Wrangle the input data into the right columns
orders = orders.selectExpr("cast(value as string)") \
	.select(from_json('value', schema).alias("value" )) \
	.select("value.*")

# Calculate following columns to help with the data analysys 
# 	1. total_cost: Total cost of an order arrived at by summing up the cost of all products in that invoice
# 	2. total_items: Total number of items present in an order
# 	3. is_order: This flag denotes whether an order is a new order or not. If this invoice is for a return order, the value should be 0.
# 	4. is_return: This flag denotes whether an order is a return order or not. If this invoice is for a new sales order, the value should be 0.
orders = orders.withColumn("total_cost", total_cost_udf(orders.type, orders.items))
orders = orders.withColumn("total_items", total_items_udf(orders.items))
orders = orders.withColumn("is_order", is_order_udf(orders.type))
orders = orders.withColumn("is_return", is_return_udf(orders.type))

# Calculating additional columns and writing the summarised input table to the console
orders_console= orders  \
	.select("invoice_no", "country", "timestamp", "total_cost", "total_items", "is_order", "is_return") \
	.writeStream  \
	.outputMode("append")  \
	.format("console")  \
	.option("truncate", "False")  \
	.trigger(processingTime="1 minute") \
	.start()

# Calculating following time-based KPIs with a watermark of 10 minutes and a tumbling window of 1 minute
# 	1. Total volume of sales
#	2. OPM (orders per minute)
# 	3. Rate of return
#	4. Average transaction size
orders_time_based_kpi= orders \
    .withWatermark("timestamp","10 minutes") \
    .groupby(window("timestamp", "1 minute")) \
    .agg(count("invoice_no").alias("OPM"),
		sum("total_cost").alias("total_sale_volume"),
        sum("is_order").alias("total_orders"),
        sum("is_return").alias("total_returns")) \
    .select("window","OPM","total_sale_volume","total_orders","total_returns")	

orders_time_based_kpi = orders_time_based_kpi.withColumn("rate_of_return", (orders_time_based_kpi.total_returns /(orders_time_based_kpi.total_orders + orders_time_based_kpi.total_returns)))
orders_time_based_kpi = orders_time_based_kpi.withColumn("average_transaction_size", (orders_time_based_kpi.total_sale_volume /(orders_time_based_kpi.total_orders + orders_time_based_kpi.total_returns)))

# Write time based KPI values to JSON files
time_based_kpi = orders_time_based_kpi \
	.select("window", "OPM", "total_sale_volume", "rate_of_return", "average_transaction_size") \
	.writeStream \
    .format("json") \
    .outputMode("append") \
    .option("truncate", "false") \
    .option("path", "time_kpi/") \
    .option("checkpointLocation", "time_kpi/checkpoint/") \
    .trigger(processingTime="1 minutes") \
    .start()

# Calculating following country-based KPIs with a watermark of 10 minutes and a tumbling window of 1 minute
# 	1. Total volume of sales
#	2. OPM (orders per minute)
# 	3. Rate of return
orders_country_based_kpi= orders \
    .withWatermark("timestamp","10 minutes") \
    .groupby(window("timestamp", "1 minute"), "country") \
    .agg(count("invoice_no").alias("OPM"),
		sum("total_cost").alias("total_sale_volume"),
        sum("is_order").alias("total_orders"),
        sum("is_return").alias("total_returns")) \
    .select("window", "country", "OPM","total_sale_volume","total_orders","total_returns")	

orders_country_based_kpi = orders_country_based_kpi.withColumn("rate_of_return", (orders_country_based_kpi.total_returns /(orders_country_based_kpi.total_orders + orders_country_based_kpi.total_returns)))	

# Write time based KPI values
country_based_kpi = orders_country_based_kpi \
	.select("window", "country", "OPM", "total_sale_volume", "rate_of_return") \
	.writeStream \
    .format("json") \
    .outputMode("append") \
    .option("truncate", "false") \
    .option("path", "country_kpi/") \
    .option("checkpointLocation", "country_kpi/checkpoint/") \
    .trigger(processingTime="1 minutes") \
    .start()	

country_based_kpi.awaitTermination()
