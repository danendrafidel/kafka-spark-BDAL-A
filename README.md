# Danendra Fidel Khansa (5027231063) Big Data and LakeHouse A

## SOAL

🎯 Latar Belakang Masalah
Sebuah perusahaan logistik mengelola beberapa gudang penyimpanan yang menyimpan barang sensitif seperti makanan, obat-obatan, dan elektronik. Untuk menjaga kualitas penyimpanan, gudang-gudang tersebut dilengkapi dengan dua jenis sensor:

- Sensor Suhu
- Sensor Kelembaban

Sensor akan mengirimkan data setiap detik. Perusahaan ingin memantau kondisi gudang secara real-time untuk mencegah kerusakan barang akibat suhu terlalu tinggi atau kelembaban berlebih.

📋 Tugas Mahasiswa

1. Buat Topik Kafka
   Buat dua topik di Apache Kafka:

   - sensor-suhu-gudang
   - sensor-kelembaban-gudang

Topik ini akan digunakan untuk menerima data dari masing-masing sensor secara real-time.

2.  Simulasikan Data Sensor (Producer Kafka)
    Buat dua Kafka producer terpisah:

    a. Producer Suhu

    - Kirim data setiap detik
    - Format:

    ```
    {"gudang_id": "G1", "suhu": 82}
    ```

    b. Producer Kelembaban

    - Kirim data setiap detik
    - Format:

    ```
    {"gudang_id": "G1", "kelembaban": 75}
    ```

Gunakan minimal 3 gudang: G1, G2, G3.

3. Konsumsi dan Olah Data dengan PySpark
   a. Buat PySpark Consumer

   - Konsumsi data dari kedua topik Kafka.

   b. Lakukan Filtering:

   - Suhu > 80°C → tampilkan sebagai peringatan suhu tinggi
   - Kelembaban > 70% → tampilkan sebagai peringatan kelembaban tinggi

Contoh Output:

```
[Peringatan Suhu Tinggi]
Gudang G2: Suhu 85°C
```

```
[Peringatan Kelembaban Tinggi]
Gudang G3: Kelembaban 74%
```

4. Gabungkan Stream dari Dua Sensor
   Lakukan join antar dua stream berdasarkan gudang_id dan window waktu (misalnya 10 detik) untuk mendeteksi kondisi bahaya ganda.

c. Buat Peringatan Gabungan:

Jika ditemukan suhu > 80°C dan kelembaban > 70% pada gudang yang sama, tampilkan peringatan kritis.

✅ Contoh Output Gabungan:
[PERINGATAN KRITIS]
Gudang G1:

- Suhu: 84°C
- Kelembaban: 73%
- Status: Bahaya tinggi! Barang berisiko rusak

Gudang G2:

- Suhu: 78°C
- Kelembaban: 68%
- Status: Aman

Gudang G3:

- Suhu: 85°C
- Kelembaban: 65%
- Status: Suhu tinggi, kelembaban normal

Gudang G4:

- Suhu: 79°C
- Kelembaban: 75%
- Status: Kelembaban tinggi, suhu aman

## Sistem Monitoring Sensor Gudang dengan Kafka dan PySpark

Sistem ini mengimplementasikan monitoring sensor suhu dan kelembaban gudang menggunakan Apache Kafka untuk streaming data dan PySpark untuk pemrosesan data real-time.

## Komponen Sistem

1. **Producer Suhu** (`producer_suhu.py`)

   - Mensimulasikan sensor suhu untuk 3 gudang (G1, G2, G3)
   - Mengirim data suhu (75-90°C) setiap detik ke topic `sensor-suhu-gudang`

2. **Producer Kelembaban** (`producer_kelembaban.py`)

   - Mensimulasikan sensor kelembaban untuk 3 gudang
   - Mengirim data kelembaban (60-80%) setiap detik ke topic `sensor-kelembaban-gudang`

3. **Consumer PySpark** (`consumer_pyspark.py`)
   - Memproses data streaming dari kedua topic
   - Melakukan analisis dan filtering:
     - Peringatan suhu tinggi (>80°C)
     - Peringatan kelembaban tinggi (>70%)
     - Status gabungan suhu dan kelembaban

## Prasyarat

- Docker
- Docker Compose

## producer_suhu.py

```py
from confluent_kafka import Producer
import json
import time
import random

conf = {'bootstrap.servers': 'kafka:9092'}  # kafka adalah nama service di docker-compose
producer = Producer(conf)

gudang_ids = ['G1', 'G2', 'G3']

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for message: {err}")
    else:
        print(msg.value().decode('utf-8'))
        #print(f"Message delivered to {msg.topic()} partition [{msg.partition()}]")


try:
    while True:
        for gudang_id in gudang_ids:
            suhu = random.randint(75, 90)
            data = {"gudang_id": gudang_id, "suhu": suhu}
            producer.produce("sensor-suhu-gudang", json.dumps(data).encode('utf-8'), callback=delivery_report)
            producer.poll(0)
        time.sleep(1)
except KeyboardInterrupt:
    print("Producer suhu stopped.")
finally:
    producer.flush()

```

## producer_kelembaban.py

```py
from confluent_kafka import Producer
import json
import time
import random

conf = {'bootstrap.servers': 'kafka:9092'}
producer = Producer(conf)

gudang_ids = ['G1', 'G2', 'G3']

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for message: {err}")
    else:
        # Decode message from bytes back to JSON string for display
        print(msg.value().decode('utf-8'))
        #print(f"Message delivered to {msg.topic()} partition [{msg.partition()}]")

try:
    while True:
        for gudang_id in gudang_ids:
            kelembaban = random.randint(60, 80)
            data = {"gudang_id": gudang_id, "kelembaban": kelembaban}
            producer.produce("sensor-kelembaban-gudang", json.dumps(data).encode('utf-8'), callback=delivery_report)
            producer.poll(0)
        time.sleep(1)
except KeyboardInterrupt:
    print("Producer kelembaban stopped.")
finally:
    producer.flush()

```

## consumer_pyspark.py

```py
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

```

## Cara Menjalankan

1.  **Clone repository dan masuk ke direktori proyek**

2.  **Jalankan container menggunakan Docker Compose**

    ```bash
    docker-compose up -d
    ```

3.  ** Masuk ke container Kafka**

    ```bash
    docker exec -it kafka bash
    ```

4.  **Buat Topik**

    ```bash
    kafka-topics --create --topic sensor-suhu-gudang --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
    kafka-topics --create --topic sensor-kelembaban-gudang --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
    ```

5.  **Cek apakah Topik tersedia?**

    ```bash
    kafka-topics --list --bootstrap-server localhost:9092
    ```

6.  **Masuk ke container PySpark**

    ```bash
    docker exec -it pyspark bash
    ```

7.  **Install dependencies**

    ```bash
    pip install -r requirements.txt
    ```

8.  **Jalankan producer dan consumer**

    - Buka terminal baru untuk setiap proses
    - Terminal 1 (Producer Suhu):
      ```bash
      python producer_suhu.py
      ```
    - Terminal 2 (Producer Kelembaban):
      ```bash
      python producer_kelembaban.py
      ```
    - Terminal 3 (Consumer PySpark):

      ```bash
      spark-submit \
       --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
       consumer_pyspark.py
      ```

9.  **Matikan Docker jika Tidak digunakan**
    ```bash
    docker-compose down
    ```

## Output

Consumer akan menampilkan:

1. Peringatan suhu tinggi
2. Peringatan kelembaban tinggi
3. Status gabungan dengan kondisi:
   - "Bahaya tinggi! Barang berisiko rusak" (suhu >80°C dan kelembaban >70%)
   - "Suhu tinggi, kelembaban normal" (suhu >80°C)
   - "Kelembaban tinggi, suhu aman" (kelembaban >70%)
   - "Aman" (kondisi normal)

## Struktur Docker

- **Zookeeper**: Port 2181
- **Kafka**: Port 9092
- **PySpark**: Port 4040 (Spark UI)

## Catatan

- Data yang dikirim adalah data simulasi
- Sistem menggunakan window time 10 detik untuk join data suhu dan kelembaban
- Semua output ditampilkan di console
