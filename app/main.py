from load import Loader
from connection import Connection
from transform import Transform
from pipeline import Pipeline
def main():
    folder_path = "/asset/CRS_Datafeed_2025-02-28_1"
    cluster_IP = "spark://localhost:7077"

    connector = Connection(cluster_IP)
    spark = connector.connect_to_spark()
    loader = Loader(folder_path, spark)
    data = loader.load_data()
    pipeline = Pipeline(data)
    pipeline.ETL()
    connector.stop_cluster(spark)


if __name__ == "__main__":
    main()