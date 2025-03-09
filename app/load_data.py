from pyspark.sql import SparkSession, DataFrame
import zipfile

class Load_Data:
    def __init__(self, data_folder_path:str, spark_session:SparkSession):
        self.data_folder_path = data_folder_path
        self.spark_session = spark_session
        self.resultant_dataframe = None
        with zipfile.ZipFile(f"{self.data_folder_path}/CRS Datafeed 2025-02-28 1.zip", 'r') as zip_ref:
            zip_ref.extractall(f"{self.data_folder_path}/CRS_Datafeed_2025-02-28_1")

    def load_Features_dataFrame(self)->None:
        self.feature_dataframe = self.spark_session.read.option("header", True).option("infer_Schema", True).csv(path="/asset/CRS_Datafeed_2025-02-28_1/Features.csv")
        self.analyze_dataframe(self.feature_dataframe)

    def load_logic_dataFrame(self)->None:
        self.logic_dataframe = self.spark_session.read.option("header", True).option("infer_Schema", True).csv(path="/asset/CRS_Datafeed_2025-02-28_1/logic.csv")
        self.analyze_dataframe(self.logic_dataframe)

    def load_Mfrs_dataFrame(self)->None:
        self.Mrfs_dataframe = self.spark_session.read.option("header", True).option("infer_Schema", True).csv(path="/asset/CRS_Datafeed_2025-02-28_1/Mfrs.csv")
        self.analyze_dataframe(self.Mrfs_dataframe)

    def load_photogallery_dataFrame(self)->None:
        self.photogallery_dataFrame = self.spark_session.read.option("header", True).option("infer_Schema", True).csv(path="/asset/CRS_Datafeed_2025-02-28_1/photogallery.csv")
        self.analyze_dataframe(self.photogallery_dataFrame)

    def load_pkgs_dataFrame(self)->None:
        self.pkgs_dataframe = self.spark_session.read.option("header", True).option("infer_Schema", True).csv(path="/asset/CRS_Datafeed_2025-02-28_1/pkgs.csv")
        self.analyze_dataframe(self.pkgs_dataframe)

    def load_Trim_dataFrame(self)->None:
        self.Trim_dataframe = self.spark_session.read.option("header", True).option("infer_Schema", True).csv(path="/asset/CRS_Datafeed_2025-02-28_1/Trims.csv")
        self.analyze_dataframe(self.Trim_dataframe)

    def analyze_dataframe(self, dataframe:DataFrame)->None:
        print(f"Dataframe:\n{dataframe.show(5)}")
        print(f"Dataframe shape: {dataframe.count(), len(dataframe.columns)}")
        print(f"Dataframe schema: {dataframe.printSchema(2)}")