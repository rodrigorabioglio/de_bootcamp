from pyspark.sql import (
    SparkSession,
    functions as F,
)

from const import SCHEMA, COLS, JAR_DIR, DATA_DIR, JAR_DIR

def prepare_data():

    spark = SparkSession\
        .builder\
        .appName("IngestToPG")\
        .master("local[*]")\
        .getOrCreate()

    # read data with schema
    df = (
        spark
        .read
        .option("sep", ";")
        .option("header", "true")
        .option("encoding", "latin1")
        .schema(SCHEMA)
        .csv(f"{DATA_DIR}/*")
    )

    # prepare_data
    prepared = df\
        .withColumn('TS_GERACAO',F.to_timestamp(F.concat(df.HH_GERACAO, F.lit(' '), df.DT_GERACAO),'dd/MM/yyy HH:mm:ss'))\
        .select(COLS)\

    # rename columns
    prepared=prepared.toDF(*[c.lower() for c in prepared.columns])

def write_to_postgres():

    spark = SparkSession\
        .builder\
        .appName("IngestToPG")\
        .master("local[*]")\
        .config("spark-jars",f"{JAR_DIR}/postgresql-42.5.1.jar")\
        .config("spark.driver.extraClassPath",f"{JAR_DIR}/postgresql-42.5.1.jar")\
        .getOrCreate()

    prepared.write.format("jdbc")\
        .option("url", "jdbc:postgresql://db:5432/ny_taxi") \
        .option("driver", "org.postgresql.Driver")\
        .option("user", "postgres")\
        .option("password", "postgres")\
        .option("dbtable", "spark_test")\
        .mode("overwrite")\
        .save()

def prepare_data_and_write_to_postgres():

    spark = SparkSession\
        .builder\
        .appName("IngestToPG")\
        .master("local[*]")\
        .config("spark.jars",f"{JAR_DIR}/postgresql-42.5.1.jar")\
        .config("spark.driver.extraClassPath",f"{JAR_DIR}/postgresql-42.5.1.jar")\
        .getOrCreate()

    # read data with schema
    df = (
        spark
        .read
        .option("sep", ";")
        .option("header", "true")
        .option("encoding", "latin1")
        .schema(SCHEMA)
        .csv(f"{DATA_DIR}/*")
    )

    # prepare_data
    prepared = df\
        .withColumn('TS_GERACAO',F.to_timestamp(F.concat(df.HH_GERACAO, F.lit(' '), df.DT_GERACAO),'dd/MM/yyy HH:mm:ss'))\
        .select(COLS)\

    # rename columns
    prepared=prepared.toDF(*[c.lower() for c in prepared.columns])

    prepared.write.format("jdbc")\
        .option("url", "jdbc:postgresql://db:5432/ny_taxi") \
        .option("driver", "org.postgresql.Driver")\
        .option("user", "postgres")\
        .option("password", "postgres")\
        .option("dbtable", "spark_test")\
        .mode("overwrite")\
        .save()
