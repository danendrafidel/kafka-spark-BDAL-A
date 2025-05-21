from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr, current_timestamp, window
from pyspark.sql.types import StructType, StringType, IntegerType

spark = SparkSession.builder \
    .appName("KafkaSensorMonitor") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Schema sensor suhu
schema_suhu = StructType() \
    .add("gudang_id", StringType()) \
    .add("suhu", IntegerType())

# Schema sensor kelembaban
schema_kelembaban = StructType() \
    .add("gudang_id", StringType()) \
    .add("kelembaban", IntegerType())

# Stream suhu dari Kafka
df_suhu = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "sensor-suhu-gudang") \
    .option("startingOffsets", "latest") \
    .load()

# Stream kelembaban dari Kafka
df_kelembaban = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "sensor-kelembaban-gudang") \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON dan tambahkan timestamp
suhu_data = df_suhu.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema_suhu).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", current_timestamp())

kelembaban_data = df_kelembaban.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema_kelembaban).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", current_timestamp())

# Filter suhu tinggi dan kelembaban tinggi, buat stream filtered
filtered_suhu = suhu_data.filter(col("suhu") > 80)
filtered_kelembaban = kelembaban_data.filter(col("kelembaban") > 70)

# Peringatan suhu tinggi
peringatan_suhu = filtered_suhu.selectExpr(
    "'[Peringatan Suhu Tinggi]' as warning_type",
    "gudang_id",
    "suhu",
    "timestamp"
)

# Peringatan kelembaban tinggi
peringatan_kelembaban = filtered_kelembaban.selectExpr(
    "'[Peringatan Kelembaban Tinggi]' as warning_type",
    "gudang_id",
    "kelembaban",
    "timestamp"
)

# Gabungkan dua stream berdasarkan gudang_id dan window 10 detik
joined_stream = suhu_data.alias("s") \
    .join(
        kelembaban_data.alias("k"),
        expr("""
            s.gudang_id = k.gudang_id AND
            s.timestamp BETWEEN k.timestamp - INTERVAL 10 seconds AND k.timestamp + INTERVAL 10 seconds
        """)
    ) \
    .selectExpr(
        "s.gudang_id as gudang_id",
        "s.suhu as suhu",
        "k.kelembaban as kelembaban"
    )

# Tambahkan kolom status sesuai kondisi
joined_status = joined_stream.withColumn(
    "status",
    expr("""
        CASE
            WHEN suhu > 80 AND kelembaban > 70 THEN 'Bahaya tinggi! Barang berisiko rusak'
            WHEN suhu > 80 THEN 'Suhu tinggi, kelembaban normal'
            WHEN kelembaban > 70 THEN 'Kelembaban tinggi, suhu aman'
            ELSE 'Aman'
        END
    """)
)

# Output ke console
query_peringatan_suhu = peringatan_suhu.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query_peringatan_kelembaban = peringatan_kelembaban.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query_gabungan = joined_status.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query_peringatan_suhu.awaitTermination()
query_peringatan_kelembaban.awaitTermination()
query_gabungan.awaitTermination()
