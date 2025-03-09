from load_data import Load_Data
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import initcap, concat_ws, col, array
import json

class Transform:
    def __init__(self, load_data:Load_Data, spark_session:SparkSession):
        self.load_data = load_data
        self.spark_session = spark_session

    def get_country_for_features_dataframe(self):
        self.load_data.feature_dataframe = self.load_data.feature_dataframe.filter(self.load_data.feature_dataframe.AttributeName == 'Manufacturer Country').withColumnRenamed('Value', 'countries').withColumn('countries', array(col('countries')))

    def drop_and_rename_columns_from_trim_dataframe(self):
        self.load_data.Trim_dataframe = self.load_data.Trim_dataframe.withColumnRenamed('ManufacturerName', 'manufacturer').withColumnRenamed('ModelYear', 'year').withColumnRenamed('MSRP', 'msrp').withColumn('model', concat_ws(" ", col("ModelName"), col("TrimName"))).withColumn('category', initcap(col("ProdType"))).withColumn('subcategory', initcap(col("ProdType"))).withColumn('description', concat_ws(" ", col("manufacturer"), col("model"))).select(['ProdType', 'TrimId', 'ModelId', 'MakeId', 'manufacturer','model','year', 'msrp', 'description', 'TrimName', 'ModelName', 'category', 'subcategory'])

    def get_columns_from_identifiers_features_dataframe(self):
        self.load_data.feature_dataframe = self.load_data.feature_dataframe.select(['TrimId', 'AttributeId', 'AttributeName', 'countries']).filter(self.load_data.feature_dataframe.FeatureName=='Identifiers')

        # Convert DataFrame to a list of dictionaries (row-wise format)
        json_data = self.create_json_output_from_dataframe(self.load_data.feature_dataframe)

        # Convert to JSON string
        json_str = json.dumps(json_data, indent=4)

        rdd = self.spark_session.sparkContext.parallelize([json_str])
        
        self.load_data.resultant_dataframe = self.spark_session.read.json(rdd)

        self.load_data.analyze_dataframe(self.load_data.resultant_dataframe)

    def join_dataframes(self, dataframe_1:DataFrame, dataframe_2:DataFrame, col:list[str], join_type="left")->DataFrame:
        try:
            dataframe_2 = dataframe_1.join(dataframe_2, on=col, how=join_type)
            self.load_data.analyze_dataframe(dataframe_2)
            return dataframe_2
        except Exception as e:
            print(e)
        return None
    
    def create_json_output_from_dataframe(self, dataframe:DataFrame):
        return [row.asDict() for row in dataframe.collect()]
    
    def add_key_value_from_resultant_dataframe_to_output(self, key:str, dataframe:DataFrame, output:dict):
        dict_ =  [row.asDict() for row in dataframe.collect()]
        if not output:
            for d in dict_:
                output.append({f"{key}":d})
        else:
            for d, e in zip(output, dict_):
                d[key] = e

    def create_output(self):
        output = []
        self.add_key_value_from_resultant_dataframe_to_output('general', self.load_data.resultant_dataframe, output)
        for d in output:
            d["meta"] = {
                "source": "CRS"
                }
        print(output[:5])