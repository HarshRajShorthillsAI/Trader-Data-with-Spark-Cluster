from pyspark.sql import SparkSession
from load_data import Load_Data
from transform import Transform

class Trader_Pipeline:
    def __init__(self, data_folder_path:str):
        self.data_folder_path=data_folder_path
        self.spark_session = SparkSession.builder.master("spark://localhost:7077").appName("Trader-Data").getOrCreate()
        self.load_data = Load_Data(self.data_folder_path, spark_session=self.spark_session)
        self.transform = Transform(self.load_data, spark_session=self.spark_session)

    def task1(self):
        self.load_data.load_features_dataFrame()
        self.load_data.load_logic_dataFrame()
        self.load_data.load_Mfrs_dataFrame()
        self.load_data.load_photogallery_dataFrame()
        self.load_data.load_pkgs_dataFrame()
        self.load_data.load_Trim_dataFrame()

    def task2(self):
        self.spark_session.conf.set("spark.sql.adaptive.enabled", "false")
        self.load_data.load_Trim_dataFrame()
        self.load_data.load_Features_dataFrame()
        self.transform.drop_and_rename_columns_from_trim_dataframe()
        self.transform.get_country_for_features_dataframe()
        self.transform.get_columns_from_identifiers_features_dataframe()
        self.load_data.resultant_dataframe = self.transform.join_dataframes(self.load_data.Trim_dataframe, self.load_data.feature_dataframe.select(["TrimId", 'countries']), col=["TrimId"], join_type="full")
        self.transform.create_output()
        
        self.spark_session.conf.set("spark.sql.adaptive.enabled", "true")
