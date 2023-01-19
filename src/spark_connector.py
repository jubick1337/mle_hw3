import configparser

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext


class SparkConnector:
    def __init__(self):
        self.db_config = configparser.ConfigParser().read('../db_config.ini')
        self.spark_config = SparkConf()
        self.spark_config.set("spark.app.name", "mle_hw3")
        self.spark_config.set("spark.master", "local")
        self.spark_config.set("spark.executor.cores", "16")
        self.spark_config.set("spark.executor.instances", "1")
        self.spark_config.set("spark.executor.memory", "16g")
        self.spark_config.set("spark.locality.wait", "0")
        self.spark_config.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        self.spark_config.set("spark.kryoserializer.buffer.max", "2000")
        self.spark_config.set("spark.executor.heartbeatInterval", "6000s")
        self.spark_config.set("spark.network.timeout", "10000000s")
        self.spark_config.set("spark.shuffle.spill", "true")
        self.spark_config.set("spark.driver.memory", "8g")
        self.spark_config.set("spark.driver.maxResultSize", "8g")
        self.spark_config.set("spark.jars", "C:\\postgresql-42.5.0.jar")  # To load psql

        self.sc = SparkContext.getOrCreate(conf=self.spark_config)

        self.postgres = SQLContext(self.sc)
        self.properties = {
            "driver": "org.postgresql.Driver",
            "user": self.db_config['POSTGRES']['username'],
            "password": self.db_config['POSTGRES']['password']
        }

    def read(self, url):
        self.postgres.read.jdbc(url=url, table='openfoodsfacts', properties=self.properties)

    def write(self, df, url):
        df.write.jdbc(url=url, table="clustering_results", mode="overwrite", properties=self.properties)
