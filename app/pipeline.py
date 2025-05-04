from transform import Transform
import json

class Pipeline:
    def __init__(self, dict_df):
        self.dict_df = dict_df
        self.final_data = None
    def ETL(self):
        transform = Transform()
        trimmed_table = transform.rename_trim_table(self.dict_df["Trims"])
        countries_table = transform.extract_country_from_feature_table(self.dict_df["Features"])
        options_table = transform.extract_options_for_trimId(self.dict_df["Features"])
        feature_table = transform.extract_features_for_trimId(self.dict_df["Features"])
        meta_table = transform.extract_meta_for_trimId(self.dict_df["Features"])
        other_table = transform.extract_feature_detail_values(self.dict_df['Features'])
        self.final_data = transform.join_tables(trimmed_table, countries_table, col=["TrimId"], join_type="full")
        self.final_data = transform.join_tables(self.final_data, options_table, col=["TrimId"], join_type="full")
        self.final_data = transform.join_tables(self.final_data, feature_table, col=["TrimId"], join_type="full")
        self.final_data = transform.join_tables(self.final_data, meta_table, col=["TrimId"], join_type="full")
        self.final_data = transform.join_tables(self.final_data, other_table, col=["TrimId"], join_type="full")
        output_data = transform.generate_output(self.final_data)
        with open('output.json', 'w') as f:
            json.dump(output_data, f, indent=4)
