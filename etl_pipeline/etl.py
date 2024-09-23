import yaml
from transformations import *
from utils import *


def main():
    try:
        
        # Load configuration data from YAML file
        with open('/Users/johnnjenga/Downloads/Zambia_ETL/config/config.yaml', 'r') as file:
            config_data = yaml.safe_load(file)
        
        # Access configuration parameters
        date_columns = config_data['date_columns']
        input_path_plus = config_data['input_path_plus']
        input_path_legacy = config_data['input_path_legacy']
        report_period = config_data['report_period'][0]

        # load data        
        scplus_df = get_csv(input_path_plus) \
                .withColumn('source', lit('SmartCare Plus'))


        legacy_df = get_csv(input_path_legacy) \
            .withColumn('source', lit('SmartCare Legacy'))
            
        with open('/Users/johnnjenga/Downloads/Zambia_ETL/config/column_mapping.yaml', 'r') as file:
            mappings = yaml.safe_load(file)

        for legacy_col, new_col in mappings['column_mappings'].items():
            scplus_df = scplus_df.withColumnRenamed(new_col, legacy_col)
        
        common_columns = [col for col in scplus_df.columns if col in legacy_df.columns]

        scplus_df_selected = scplus_df.select(common_columns)
        legacy_df_selected = legacy_df.select(common_columns)


        combined_df = scplus_df_selected.union(legacy_df_selected)

        # Apply transformations
        df = (combined_df
            .transform(lambda df: convert_dates(df, date_columns, report_period))
            .transform(lambda df: get_month_year(df, date_columns))
            .transform(process_patient_data)
            .transform(lambda df: age_group(df, 'age_at_art_initiation', 'age_groupt_Art_initiation'))
            .transform(weight_category)
            .transform(BMI_category)
            .transform(linkage_to_care_category)
            .transform(add_ART_cohort)
            .transform(lambda df: quarters(df, 'ARTStartDate_month', 'ART_start_quart'))
            .transform(lambda df: quarters(df, 'report_period_month', 'reporting_quarter'))
            .transform(vl_eligibility)
            .transform(lambda df: cd4_group(df, 'baseline_cd4', 'basecd4group'))
            .transform(lambda df: cd4_percentage(df, 'baseline_cd4_perc', 'basecd4_perc'))
            .transform(days_between_visits)
            .transform(generate_TX_CURR)
            .transform(patient_status)
            .transform(final_clinical_outcome)
            .transform(generate_outcome_1)
            .transform(vl_coverage)
            .transform(categorize_vl)
            .transform(hiv_cascade)
            .transform(calculate_tx_ml)
            .transform(silent_transfer)
            .transform(calculate_mmd_proxy_mo)
            .transform(calculate_mmd_type)
            .transform(calculate_dsd_new)
            # .transform(calculate_mms) 
            .transform(deduplication) 
            )
        
        print('DONE EXECUTING!')
        
        
        # save the dataframe to db below. Make changes and uncomment
        # persist_dataframe('db_name', 'table_name', df)
        
        
    except Exception as e:
        print("An error occurred:", str(e))

if __name__ == "__main__":
    main()
