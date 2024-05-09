import yaml
from transformations import *
from utils import *


def main():
    try:
        
        # Load configuration data from YAML file
        with open('../config/config.yaml', 'r') as file:
            config_data = yaml.safe_load(file)
            
        # Access configuration parameters
        date_columns = config_data['date_columns']
        input_path = config_data['input_path']
        report_period = config_data['report_period'][0]

        # Load data
        df = get_csv(input_path)

        # Apply transformations
        df = (df
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
            # .transform(cd4_group)
            # .transform(cd4_percentage)
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
            )
        
        # save the dataframe to db below. Make changes and uncomment
        # persist_dataframe('db_name', 'table_name', df)
        
        
    except Exception as e:
        print("An error occurred:", str(e))

if __name__ == "__main__":
    main()
