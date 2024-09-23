from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from utils import *

def convert_dates(df, date_columns, report_period_value):
    """
    Convert date columns in the DataFrame to a standardized format.
    
    Args:
        df (DataFrame): Input DataFrame.
        date_columns (list): List of column names containing dates to be converted.
        report_period_value (str): The report period value in 'yyyy-MM-dd' format.
        
    Returns:
        DataFrame: DataFrame with date columns converted to a standardized format.
    """
    
    # Convert the report period value to a date
    df = df.withColumn("report_period", F.to_date(F.lit(report_period_value), 'yyyy-MM-dd'))
    
    # Iterate through date columns and convert them to 'yyyy-MM-dd' format
    for column in date_columns:
        df = df.withColumn(column, to_date(col(column), 'yyyy-MM-dd'))
        
    return df


def get_month_year(df, date_columns):
    """
    Extract day, month, and year components from date columns in the DataFrame.
    
    Args:
        df (DataFrame): Input DataFrame.
        date_columns (list): List of column names containing dates to extract components from.
        
    Returns:
        DataFrame: DataFrame with day, month, and year components extracted from date columns.
    """
    for column in date_columns:
        df = df.withColumn(column + '_day', date_format(column, 'EEEE'))
        df = df.withColumn(column + '_month', date_format(column, 'MMMM'))
        df = df.withColumn(column + '_year', year(column))
        
    return df


def age_group(df, age_col_name, new_col_name):
    """
    Assign age groups based on the values in the age column.

    Args:
        df (DataFrame): Input DataFrame.
        age_col_name (str): Name of the column containing age values.
        new_col_name (str): Name of the new column to store the age groups.

    Returns:
        DataFrame: DataFrame with the age groups assigned.
    """
    df = df.withColumn(new_col_name,
                       when((col(age_col_name) > 0) & (col(age_col_name) <= 0.999), '0<1')
                       .when((col(age_col_name) >= 1) & (col(age_col_name) <= 4), '1-4')
                       .when((col(age_col_name) >= 5) & (col(age_col_name) <= 9), '5-9')
                       .when((col(age_col_name) >= 10) & (col(age_col_name) <= 14), '10-14')
                       .when((col(age_col_name) >= 15) & (col(age_col_name) <= 19), '15-19')
                       .when((col(age_col_name) >= 20) & (col(age_col_name) <= 24), '20-24')
                       .when((col(age_col_name) >= 25) & (col(age_col_name) <= 29), '25-29')
                       .when((col(age_col_name) >= 30) & (col(age_col_name) <= 34), '30-34')
                       .when((col(age_col_name) >= 35) & (col(age_col_name) <= 39), '35-39')
                       .when((col(age_col_name) >= 40) & (col(age_col_name) <= 44), '40-44')
                       .when((col(age_col_name) >= 45) & (col(age_col_name) <= 49), '45-49')
                       .when((col(age_col_name) >= 50) & (col(age_col_name) <= 54), '50-54')
                       .when((col(age_col_name) >= 55) & (col(age_col_name) <= 59), '55-59')
                       .when((col(age_col_name) >= 60) & (col(age_col_name) <= 64), '60-64')
                       .when(col(age_col_name) >= 65, '65+')
                       .when((col(age_col_name) >= -50) & (col(age_col_name) <= -1), 'BIF'))

    return df


def process_patient_data(df):
    '''
    Process patient data to calculate BMI, update HIV test results, gender, PMTCT status, and ART status.

    Args:
        df (DataFrame): Input DataFrame containing patient data.

    Returns:
        DataFrame: DataFrame with updated columns including BMI, HIV test results, gender, PMTCT status, and ART status.
    '''
    df = df.withColumn('patientheight', F.when(col('patientheight').isNotNull(), col('patientheight') / 100)) \
        .withColumn('BMI', F.when((col('patientheight').isNotNull()) & (col('patientweight').isNotNull()), col('patientweight') / (col('patientheight') * col('patientheight')))) \
           .withColumn('BMI', F.format_number(col('BMI'), 1)) \
           .withColumn('HIVtest_results',
                       F.when((F.col('hivtestresults') != 'Positive') & (F.col('hivtestresults') != 'Negative'), 'Missing').otherwise(col('hivtestresults'))) \
           .withColumn('sex', F.when(col('sex') == 'F', 'Female')
                       .when(df['sex'] == 'M', 'Male')
                       .otherwise('99')) \
           .withColumn('PMTCT_TX', 
                       F.when(col('ANCVisitDate').isNotNull(), 'Yes')
                        .otherwise('No')) \
           .withColumn('OnART', 
                       F.when((col("artnumber").isNotNull()) | (col("artstartdate").isNotNull()) | (col("currentartregimen").isNotNull()), 'Yes')
                        .otherwise('No')) \
           .withColumn('age_at_art_initiation', calculate_age('ARTStartDate','dob')) \
            .withColumn('age_at_first_visit', calculate_age('FirstInteraction_date','dob')) \
            .withColumn('current_age', calculate_age(current_date(),'dob'))
    return df


def weight_category(df):
    """
    Assign weight categories based on patient weights.

    Args:
        df (DataFrame): Input DataFrame containing patient weight data.

    Returns:
        DataFrame: DataFrame with the 'weight_category' column added.
    """
    df = df.withColumn('weight_category', 
                       when(col('patientweight').between(0, 2.999), '0<3')
                       .when(col('patientweight').between(3, 5.99), '3-5.9')
                       .when(col('patientweight').between(6, 9.99), '6-9.9')
                       .when(col('patientweight').between(10, 19.99), '10-19.9')
                       .when(col('patientweight').between(20, 29.99), '20-29.9')
                       .when(col('patientweight') >= 30, '30+'))
    
    return df


def BMI_category(df):
    df = df.withColumn("BMI_category", 
                       F.when(df["BMI"] < 18.5, "<18.5")
                        .when((df["BMI"] >= 18.5) & (df["BMI"] < 25), "18.5-24.9")
                        .when((df["BMI"] >= 25) & (df["BMI"] < 30), "25-29.9")
                        .when((df["BMI"] >= 30) & (df["BMI"] < 35), "30-34.9")
                        .when((df["BMI"] >= 35) & (df["BMI"] < 40), "35-39.9")
                        .when(df["BMI"] >= 40, "40+"))
    
    return df


def linkage_to_care_category(df):
    df = df.withColumn("linkage_to_care_category", 
                       F.when((F.datediff(df['ARTStartDate'], df['HivTestDate']).between(0, 7)), "<7")
                        .when((F.datediff(df['ARTStartDate'], df['HivTestDate']).between(8, 14)), "8-14")
                        .when((F.datediff(df['ARTStartDate'], df['HivTestDate']).between(15, 30)), "15-30")
                        .when((F.datediff(df['ARTStartDate'], df['HivTestDate']).between(31, 90)), "31-90")
                        .when((F.datediff(df['ARTStartDate'], df['HivTestDate']) >= 90), "90+"))
    
    return df


def add_ART_cohort(df):

    df = df.withColumn("months_since_art_start", 
                   F.floor(F.datediff(F.col("report_period"), F.col("ARTStartDate")) / 30.437))
    # Recode months_since_art_start into ART_cohort groups
    df = df.withColumn("ART_cohort", 
                       F.when(F.col("months_since_art_start").between(0, 3), "(0<3)")
                        .when(F.col("months_since_art_start").between(4, 6), "(4-6)")
                        .when(F.col("months_since_art_start").between(7, 12), "(7-12)")
                        .when(F.col("months_since_art_start").between(13, 24), "(13-24)")
                        .when(F.col("months_since_art_start").between(25, 36), "(25-36)")
                        .when(F.col("months_since_art_start").between(37, 48), "(37-48)")
                        .when(F.col("months_since_art_start").between(49, 60), "(49-60)")
                        .when(F.col("months_since_art_start").between(61, 72), "(61-72)")
                        .when(F.col("months_since_art_start").between(73, 84), "(73-84)")
                        .when(F.col("months_since_art_start").between(85, 96), "(85-96)")
                        .when(F.col("months_since_art_start").between(97, 108), "(97-108)")
                        .when(F.col("months_since_art_start").between(109, 120), "(109-120)")
                        .when(F.col("months_since_art_start").between(121, 132), "(121-132)")
                        .when(F.col("months_since_art_start").between(133, 144), "(133-144)")
                        .otherwise("(145+)"))

    return df



def quarters(df, month_column, new_quarter_column):
    from pyspark.sql.functions import when, col

    df = df.withColumn(
        new_quarter_column,
        when(col(month_column).isin(['October', 'November', 'December']), 'Q1')
        .when(col(month_column).isin(['January', 'February', 'March']), 'Q2')
        .when(col(month_column).isin(['April', 'May', 'June']), 'Q3')
        .when(col(month_column).isin(['July', 'August', 'September']), 'Q4')
    )

    return df


def cd4_group(df, cd4_column, cd4_group_col_name):
    df = df.withColumn(cd4_group_col_name,
                       when(col(cd4_column) <= 50, "<=50")
                       .when((col(cd4_column) > 50) & (col(cd4_column) <= 200), "51-200")
                       .when((col(cd4_column) > 200) & (col(cd4_column) <= 350), "201-350")
                       .when((col(cd4_column) > 350) & (col(cd4_column) <= 500), "351-500")
                       .when((col(cd4_column) > 500) & (col(cd4_column) <= 1000), "501-1000")
                       .when(col(cd4_column) > 1000, ">1000")
                       .otherwise(None))

    return df


def cd4_percentage(df, baseline_cd4_perc_column, new_column_name):
    df = df.withColumn(new_column_name,
                       when(col(baseline_cd4_perc_column) < 15, "<15%")
                       .when((col(baseline_cd4_perc_column) >= 15) & (col(baseline_cd4_perc_column) <= 24), "15-24%")
                       .otherwise(">=25%"))

    return df


def vl_eligibility(df):
    df = df.withColumn('VL_eligibility',
                       when(
                           ((col('months_since_art_start') >= 6) & (col('ANCVisitDate').isNull())) |
                           ((col('months_since_art_start') >= 3) & (col('ANCVisitDate').isNotNull()) | (datediff(col('report_period'), col('EstimatedDateOfDelivery'))/30.437 <= 27)),
                           'Yes'
                       )
                       .otherwise('No'))

    return df


def days_between_visits(df):
    # Define the window specification
    window_spec = Window.partitionBy("artnumber", "dob", "sex", "current_age", "ARTStartDate", "LastInteraction_date")

    # Calculate the minimum and maximum visit dates for each patient
    df = df.withColumn("old_visit", F.min("LastInteraction_date").over(window_spec)) \
           .withColumn("new_visit", F.max("LastInteraction_date").over(window_spec))

    # Calculate the difference in days between the new and old visit dates
    df = df.withColumn("visit_diff_days", F.datediff(F.col("new_visit"), F.col("LastInteraction_date")))    
    return df


def generate_TX_CURR(df):
    df = df.withColumn("Days_missed", datediff(col('report_period'), col("NextAppointmentDate"))) \
        .withColumn("Days_Away", datediff(col('report_period'), 'new_visit')) \
        .withColumn("Days_Away_Dispense", datediff(col('report_period'), col('CurrentARTRegimenDispensationDate')))

    return df


def patient_status(df):
    df = df.withColumn("patient_status",
                       when(
                              ((
                                  (col("patient_status").isNull()) |
                                (
                                    (
                                        (col("patient_status").isin('died','stoppedART','reactivated', 'made_inactive','transout')) &
                                        (
                                            (col("Patient_status_date") < col("LastInteraction_date")) |
                                            (col("Patient_status_date").isNull())
                                            )
                                        )
                                    )
                              )
                               &
                                (
                                    ((col("Days_missed") <= 30) | (col("Days_Away") <= 30) | (col("Days_Away_Dispense") <= 30)) |
                                    ((col("Days_Away_Dispense").isNull()) & ((col("Days_Away") <= 90) | (col("Days_missed").isNull()))) |
                                    ((col("Days_Away_Dispense").isNull()) & ((col("Days_Away") <= 90) | (col("Days_missed") <= 30))) |
                                    ((col("Days_missed").isNull()) & ((col("Days_Away") <= 90) | (col("Days_Away_Dispense").isNull()))) |
                                    ((col("Days_missed").isNull()) & ((col("Days_Away") <= 90) | (col("Days_Away_Dispense") <= 90)))
                                    )
                                ), 'Active')
                       .when((col("patient_status") == "died") & ((col("Patient_status_date") >= col("LastInteraction_date"))), 'Died')
                       .when((col("patient_status") == "reactived") & ((col("Patient_status_date") >= col("LastInteraction_date"))), 'Reactivated')
                       .when((col("patient_status") == "stoppedART") & ((col("Patient_status_date") >= col("LastInteraction_date"))), 'Stopped ART')
                       .when((col("patient_status") == "transout") & ((col("Patient_status_date") >= col("LastInteraction_date"))), 'Transferred-out')
                       .when((col("patient_status") == "made_inative") & ((col("Patient_status_date") >= col("LastInteraction_date"))), 'Made_inactive')
                       .otherwise(None))

    return df

# label define pt_status_new 1"Active" 2"Died" 3"Reactivated" 4"Stopped ART" 5"Transferred-out" 6"Made_inactive"


def final_clinical_outcome(df):
    df = df.withColumn("outcome",
                       when(
                           (col("patient_status") == "Active") |
                           (
                               ((col("patient_status") == "Reactivated") | (col("patient_status") == "Transferred-out")) &
                               (
                                   ((col("Days_missed") <= 30) | (col("Days_Away") <= 30) | (col("Days_Away_Dispense") <= 30)) |
                                   ((col("Days_Away_Dispense").isNull()) & ((col("Days_Away") <= 90) | (col("Days_missed").isNull()))) |
                                   ((col("Days_Away_Dispense").isNull()) & ((col("Days_Away") <= 90) | (col("Days_missed") <= 30))) |
                                   ((col("Days_missed").isNull()) & ((col("Days_Away") <= 90) | (col("Days_Away_Dispense").isNull()))) |
                                   ((col("Days_missed").isNull()) & ((col("Days_Away") <= 90) | (col("Days_Away_Dispense") <=90 )))
                                   )
                               ),
                           'retention in care').otherwise('attrition')
                       )
    return df


def generate_outcome_1(df):
    df = df.withColumn("outcome_1",
                       when((col("outcome") == 'retention in care') & (col("Patient_status_date").isNotNull()), 'Active on ART')
                       .when(((col("outcome") == 'retention in care')) & (col("Patient_status_date") < col('LastInteraction_date')),'TX_RTT')
                       .when((col("outcome") == 'attrition') | (col("patient_status").isin(["Died","Stopped ART", "Transferred-out", "Made_inactive"]) | col("patient_status").isNull()) & (col("Patient_status_date") < col('LastInteraction_date')), 'TX_ML')
                       .when(((col("outcome") == 'attrition') & (col("patient_status") == 'Transferred-out')) &
                             (
                                 ((col("Days_missed") <= 30) | (col("Days_Away") <= 30) | (col("Days_Away_Dispense") <= 30)) |
                                 ((col("Days_Away_Dispense").isNull()) & ((col("Days_Away") <= 90) | (col("Days_missed").isNull()))) |
                                 ((col("Days_Away_Dispense").isNull()) & ((col("Days_Away") <= 90) | (col("Days_missed") <= 30))) |
                                 ((col("Days_missed").isNull()) & ((col("Days_Away") <= 90) | (col("Days_Away_Dispense").isNull()))) |
                                 ((col("Days_missed").isNull()) & ((col("Days_Away") <= 90) | (col("Days_Away_Dispense") <=90 )))
                                 ), 'IIT')
                       .when((col("outcome") == 'retention in care') & ((col("ART_start_quart") == col('reporting_quarter')) & (col('ARTStartDate_year') == col('report_period_year'))), 'NET_NEW')
    )
    return df


def vl_coverage(df):
    df = df.withColumn("VL_coverage",
                       when(
                             (col("outcome") == 'retention in care') &
                             (col("VL_eligibility") == "Yes") & 
                             (datediff(col('report_period'), col("Current_VL_date")) / 30.437 <= 12),
                             'Yes')
                       .otherwise("No"))
    return df

def categorize_vl(df):
    df = df.withColumn('VL_category',
                       when((col('Current_VL_Copies') < 1000), "(<1000)")
                       .when((col('Current_VL_Copies') >= 1000) & (col('Current_VL_Copies') <= 2500), "(1000-2500)")
                       .when((col('Current_VL_Copies') > 2500), "(>2500)"))

    return df


def hiv_cascade(df):
    '''
    Calculate HIV cascade based on various conditions.
    
    Args:
        df (DataFrame): Input DataFrame containing relevant columns for HIV cascade calculation.

    Returns:
        DataFrame: DataFrame with the 'hiv_cascade' column added.
    '''
    # df = df.withColumn('hiv_cascade',
    #                    when(
    #                        (col('OnArt') == 'Yes') & (col('HIVtest_results').isin(['Positive', 'Negative', 'Missing'])),
    #                        'HIV positive')
    #                    .when(
    #                        (col('OnArt') == 'Yes') & (col('outcome') == 'retention in care'),
    #                        'Actively on ART')
    #                    .when(
    #                        (col('OnArt') == 'Yes') & (col('outcome') == 'retention in care') & (col('VL_eligibility') == 'Yes'),
    #                        'VL eligible')
    #                    .when(
    #                        (col('OnArt') == 'Yes') & (col('outcome') == 'retention in care') & (col('VL_coverage') == 'Yes'),
    #                        'VL coverage')
    #                    .when(
    #                        (col('OnArt') == 'Yes') & (col('outcome') == 'retention in care') & (col('VL_coverage') == 'Yes') & (col('VL_category') == '(<1000)'),
    #                        'VL suppression')
    #                    )
    df = df.withColumn('hiv_cascade',
                       when(
                           (col('OnArt') == 'Yes') & (col('HIVtest_results').isin(['Positive', 'Negative', 'Missing'])),
                           'HIV positive'))
    df=df.withColumn('hiv_cascade',when(
                           (col('OnArt') == 'Yes') & (col('outcome') == 'retention in care'),
                           'Actively on ART').otherwise(col('hiv_cascade')))
    df=df.withColumn('hiv_cascade',when(
                           (col('OnArt') == 'Yes') & (col('outcome') == 'retention in care') & (col('VL_eligibility') == 'Yes'),
                           'VL eligible').otherwise(col('hiv_cascade')))
    df=df.withColumn('hiv_cascade',when(
                            (col('OnArt') == 'Yes') & (col('outcome') == 'retention in care') & (col('VL_coverage') == 'Yes'),
                           'VL coverage').otherwise(col('hiv_cascade')))
    df=df.withColumn('hiv_cascade',when(
                            (col('OnArt') == 'Yes') & (col('outcome') == 'retention in care') & (col('VL_coverage') == 'Yes') & (col('VL_category') == '(<1000)'),
                           'VL suppression').otherwise(col('hiv_cascade')))
    return df


def calculate_tx_ml(df):
    '''
    Calculate TX_ML based on various conditions.

    Args:
        df (DataFrame): Input DataFrame containing relevant columns for TX_ML calculation.

    Returns:
        DataFrame: DataFrame with the 'TX_ML' column added.
    '''
    df = df.withColumn('TX_ML',
                       when(
                           col('patient_status') == 'Died',
                           'Died')
                       .when(
                           (col('outcome') == 'attrition') & (col('months_since_art_start') < 3) & (~col('patient_status').isin(['Died', 'Stopped ART'])),
                           'IIT <3 months TX')
                       .when(
                           (col('outcome') == 'attrition') & (col('months_since_art_start') >= 3) & (~col('patient_status').isin(['Died', 'Stopped ART'])),
                           'IIT 3>= months TX')
                       .when(
                           (col('outcome') == 'attrition') & (col('patient_status') == 'Stopped ART'),
                           'Stopped TX')
                       .when(
                           col('outcome') == 'retention in care',
                           'TX_CURR')
                       )
    return df


def silent_transfer(df):
    df = df.withColumn("silent_transfer",
                       when(
                           (col("artfacilityname") != col("facilityname")) & (col("artfacilityname") != "") & (col("facilityname") != "") & (col("patient_status") != 'Transferred-out'),
                           'Yes')
                       .otherwise('No'))

    df = df.withColumn("re_initiated",
                       when((col("LastInteraction_date") == col('ARTStartDate')) & (col("Silent_transfer") == 'Yes'), 'Yes')
                       .otherwise('No'))

    return df


def calculate_mmd_proxy_mo(df):

    df = df.withColumn("MMD_proxy_Mo",
                       when(col("NextAppointmentDate").isNotNull() & col("CurrentARTRegimenDispensationDate").isNotNull(),
                            datediff(col("NextAppointmentDate"), col("CurrentARTRegimenDispensationDate")) / 30.44
                            )
                       .when(col("CurrentARTRegimenDispensationDate").isNull(),
                             datediff(col("NextAppointmentDate"), col("LastInteraction_date")) / 30.44).cast('int')
                       )
    return df



def calculate_mmd_type(df):

    df = df.withColumn("MMD_type",
                       when((col('MMD_proxy_Mo') > 0) & (col('MMD_proxy_Mo') < 3), '<3MMD')
                       .when((col('MMD_proxy_Mo') >= 3) & (col('MMD_proxy_Mo') <= 5), '3-5MMD')
                       .when((col('MMD_proxy_Mo') == 6), '6MMD')
                       .when((col('MMD_proxy_Mo') >= 7) & (col('MMD_proxy_Mo') <= 11), '7-11MMD')
                       .when((col('MMD_proxy_Mo') >= 12), '12+MMD')
                       )
    return df


def calculate_dsd_new(df):
    '''
    Calculate DSD) new based on DSD values.

    Args:
        df (DataFrame): Input DataFrame containing relevant columns for DSD new calculation.

    Returns:
        DataFrame: DataFrame with the 'DSD_new' column added.
    '''
    df = df.withColumn("DSD_new",
                       when(col('DSDModel') == 'Community ART Distribution Points', 'Community ART Distribution Points')
                       .when(col('DSDModel') == 'Community Adherance Groups', 'Community Adherance Groups')
                       .when(col('DSDModel') == 'Fast Track', 'Fast Track')
                       .when(col('DSDModel') == 'Health Post', 'Health Post')
                       .when(col('DSDModel') == 'Mobile ART distritbution model', 'Mobile ART distritbution model')
                       .when(col('DSDModel') == 'Multi Month Scriptiing', 'Multi Month Scriptiing')
                       .when(col('DSDModel') == 'Multi Month Scriptiing ', 'Multi Month Scriptiing')
                       .when(col('DSDModel') == 'Rural Adherance Group', 'Rural Adherance Group')
                       .when(col('DSDModel') == 'Urban Adherance Group', 'Urban Adherance Group')
                       .otherwise(col('DSDModel'))
                      )
    return df


def calculate_mms(df):
    '''
    Calculate MMS based on MMD proxy and DSD new values.

    Args:
        df (DataFrame): Input DataFrame containing relevant columns for MMS calculation.

    Returns:
        DataFrame: DataFrame with the 'MMS' column added.
    '''
    df = df.withColumn('MMS', 
                       when((col('MMD_proxy_Mo') == 6) | (col('DSD_new') == 6), 'MMS6')
                       .when((col('MMD_proxy_Mo') == 3) | (col('DSD_new') == 3), 'MMS3')
                       .when((col('MMS') == '0') & (col('DSD_new') == 1), 'Community ART Distribution Points')
                       .when((col('MMS') == '0') & (col('DSD_new') == 2), 'Community Adherance Groups')
                       .when((col('MMS') == '0') & (col('DSD_new') == 3), 'Fast Track')
                       .when((col('MMS') == '0') & (col('DSD_new') == 4), 'Health Post')
                       .when((col('MMS') == '0') & (col('DSD_new') == 5), 'Mobile ART distritbution model')
                       .when((col('MMS') == '0') & (col('DSD_new') == 7), 'Rural Adherance Group')
                       .when((col('MMS') == '0') & (col('DSD_new') == 8), 'Urban Adherance Group')
                 )
    
    return df

def deduplication(df):
    # Unify the guid values using patient_guid and owningguid
    df = df.withColumn("_patient_guid", col("patient_guid").cast("string"))
    df = df.withColumn("pt_GUID", col("owningguid"))

    # Generate distinct flags
    window_spec = Window.partitionBy("owningguid", "dob", "sex", "current_age", "ARTStartDate").orderBy(lit(1))
    df = df.withColumn("distinct_guid", (row_number().over(window_spec) == 1).cast("int"))

    window_spec_art = Window.partitionBy("artnumber", "dob", "sex", "current_age", "ARTStartDate").orderBy(lit(1))
    df = df.withColumn("distinct_ART", (row_number().over(window_spec_art) == 1).cast("int"))

    window_spec_nupn = Window.partitionBy("nupn", "dob", "sex", "current_age", "ARTStartDate").orderBy(lit(1))
    df = df.withColumn("distinct_NUPN", (row_number().over(window_spec_nupn) == 1).cast("int"))

    window_spec_guid = Window.partitionBy("pt_GUID", "dob", "sex", "current_age", "ARTStartDate", "firstname", "lastname").orderBy(lit(1))
    df = df.withColumn("distinct_GUID", (row_number().over(window_spec_guid) == 1).cast("int"))

    window_spec_guid_n = Window.partitionBy("pt_GUID", "dob", "sex", "current_age", "ARTStartDate").orderBy(lit(1))
    df = df.withColumn("distinct_GUID_n", (row_number().over(window_spec_guid_n) == 1).cast("int"))

    # # Labeling distinct variables (adding meaningful labels as comments)
    df = df.withColumn("distinct_GUID_label", when(col("distinct_GUID") == 1, "Unique").otherwise("Duplicate"))
    df = df.withColumn("distinct_ART_label", when(col("distinct_ART") == 1, "Unique").otherwise("Duplicate"))
    df = df.withColumn("distinct_NUPN_label", when(col("distinct_NUPN") == 1, "Unique").otherwise("Duplicate"))

    # # Tagging duplicates by pt_GUID, art_start_date, and demographics
    window_spec_dup = Window.partitionBy("pt_GUID", "firstname", "lastname", "dob", "sex", "current_age", "ARTStartDate").orderBy(lit(1))
    df = df.withColumn("Pt_GUID_dups", row_number().over(window_spec_dup) - 1)

    df = df.withColumn("Pt_GUID_dups_label", when(col("Pt_GUID_dups") == 0, "No duplicate")
                                            .when(col("Pt_GUID_dups") == 1, "Duplicated once")
                                            .when(col("Pt_GUID_dups") == 2, "Duplicated twice")
                                            .when(col("Pt_GUID_dups") == 3, "Duplicated thrice"))
    
    return df
