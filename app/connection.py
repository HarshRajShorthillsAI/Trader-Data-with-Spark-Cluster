from pyspark.sql import SparkSession

class Connection():
    def __init__(self, cluster_IP):
        self.cluster_IP = cluster_IP

    def connect_to_spark(self):
        spark = SparkSession.builder \
                .master(self.cluster_IP) \
                .appName("Trader-pipeline") \
                .getOrCreate()
        spark.conf.set("spark.sql.adaptive.enabled", "false")


        return spark
    
    def stop_cluster(self, spark):
        spark.stop()