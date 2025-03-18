class Loader:
    def __init__(self, folder_path, spark):
        self.folder_path = folder_path
        self.spark = spark
        self.dict_df = {
            "Features": None,
            "logic": None,
            "Mfrs": None,
            "photogallery": None,
            "pkgs": None,
            "Trims": None
        }

    def load_data(self):
        for key in self.dict_df:
            temp_df =self.spark.read \
                        .option("header", "true") \
                        .csv(self.folder_path + "/" + key + ".csv")
            self.dict_df[key] = temp_df
        return self.dict_df
