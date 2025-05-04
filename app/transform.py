from pyspark.sql.functions import initcap, concat_ws, col, array, collect_set, struct, array_sort, to_json, expr
from pyspark.sql import DataFrame
import json

class Transform:
    def __init__(self):
        pass
    
    def rename_trim_table(self, trim):
        trim_dataframe = (
            trim.withColumnRenamed('ManufacturerName', 'manufacturer')\
            .withColumnRenamed('ModelYear', 'year')\
            .withColumnRenamed('MSRP', 'msrp')\
            .withColumn('model', concat_ws(" ", col("ModelName"), col("TrimName")))\
            .withColumn('category', initcap(col("ProdType")))\
            .withColumn('subcategory', initcap(col("ProdType")))\
            .withColumn('description', concat_ws(" ", col("manufacturer"), col("model")))\
            .select(['ProdType', 'TrimId', 'ModelId', 'MakeId', 'manufacturer','model','year', 'msrp', 'description', 'TrimName', 'ModelName', 'category', 'subcategory'])
        )
        # self.loaded_data.analyze_dataframe(trim_dataframe)
        return trim_dataframe
    
    def extract_country_from_feature_table(self, feature):
        feature_dataframe = (
            feature.filter(feature.AttributeName == 'Manufacturer Country')\
            .withColumnRenamed('Value', 'countries')\
            .withColumn('countries', array(col('countries')))
        )

        dropped_columns = ['PackageId', 'AttributeId', 'FeatureName', 'AttributeName']
        feature_dataframe = feature_dataframe.drop(*dropped_columns)
        
        return feature_dataframe
    
    def extract_options_for_trimId(self, feature):
        filtered_feature = feature.filter(feature.Value == 'Optional')\
            .select(['TrimId', 'FeatureName']).groupBy('TrimId').agg(collect_set('FeatureName').alias('Options'))
        return filtered_feature
    
    def extract_features_for_trimId(self, feature):
        filtered_feature = feature.filter(feature.Value == 'Standard')\
            .select(['TrimId', 'FeatureName']).groupBy('TrimId').agg(collect_set('FeatureName').alias('Features'))
        
        return filtered_feature
    
    def extract_meta_for_trimId(self, feature):
        filtered_meta = feature.filter(feature.FeatureName == 'Identifiers')\
            .filter(feature.AttributeName == 'Data Provider')\
            .select(['TrimId', 'Value'])\
            .withColumnRenamed('Value', 'Meta')
        
        return filtered_meta

    def convert_nested_array_to_json(self, feature:DataFrame):
        # Correct transformation to JSON
        return feature.withColumn(
            "Details",
            to_json(
                expr(
                    """
                    map_from_arrays(
                        transform(Details, detail -> detail.FeatureName),
                        transform(Details, detail -> named_struct('label', detail.AttributeName, 'desc', detail.Value))
                    )
                    """
                )
            )
        )

    def extract_feature_detail_values(self, feature:DataFrame):
        filtered_feature = (
            feature.select(['TrimId', 'FeatureName', 'Value', 'AttributeName'])
            .groupBy('TrimId', 'FeatureName')
            .agg(expr("last(AttributeName) as AttributeName"), expr("last(Value) as Value"))
            .groupBy('TrimId')
            .agg(array_sort(collect_set(struct('FeatureName', 'AttributeName', 'Value'))).alias('Details'))
        )

        filtered_feature = self.convert_nested_array_to_json(filtered_feature)

        return filtered_feature
    def join_tables(self, df1:DataFrame, df2:DataFrame, col, join_type):
        try:
            dataframe_3 = df1.join(df2, on=col, how=join_type)
            return dataframe_3
        except Exception as e:
            print(e)
        return None
    
    def generate_output(self, final_data):

        rows = final_data.collect()
        result = []

        for row in rows:
            # trimId = row["TrimId"]

            product_detail = {
                "general" : {
                    "ProdType": row["ProdType"].upper() if row["ProdType"] else None,
                    "TrimId": row["TrimId"] if row["TrimId"] else None,
                    "ModelId": row["ModelId"] if row["ModelId"] else None,
                    "MakeId": row["MakeId"] if row["MakeId"] else None,
                    "manufacturer": row["manufacturer"] if row["manufacturer"] else None,
                    "model": row["model"] if row["model"] else None,
                    "year": row["year"] if row["year"] else None,
                    "msrp": row["msrp"] if row["msrp"] else None,
                    "description": row["description"] if row["description"] else None,
                    "TrimName": row["TrimName"] if row["TrimName"] else None,
                    "ModelName": row["ModelName"] if row["ModelName"] else None,
                    "category": row["category"] if row["category"] else None,
                    "subcategory": row["subcategory"] if row["subcategory"] else None,
                    "countries": row["countries"] if row["countries"] else None,
                },
                "meta": row["Meta"] if row["Meta"] else None,
                "options": row["Options"] if row["Options"] else None,
                "features": row["Features"] if row["Features"] else None,
            }

            # Merge details into product_detail
            if row["Details"]:
                product_detail.update({
                    f"{feature}": value for feature, value in json.loads(row["Details"]).items()
                })

            result.append(product_detail)

        return result