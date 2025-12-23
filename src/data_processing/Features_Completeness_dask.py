# -------------------------------------------------------------------------------------------------
# Gemaakt door: Maarten Baas, Rens Spierings
# Datum laatste aanpassing 18-12-2025
# Dit Python script is gemaakt voorhet checken van de compleetheid van een dataset.
# het script is onderdeel van de eindopdracht voor de opleiding Data and AI Engineering van de EQI
# -------------------------------------------------------------------------------------------------

# import modules

#import pandas as pd
import dask.dataframe as dd
import json
import glob
import os


# Input parameters 
 
parquet_folder = r"K:\CentraalDatamanagement\PDC\01_WIP\01_Algemeen\X_000002_DatakwaliteitBaseline\DAMO_H_parquet"
output_map = r"K:\CentraalDatamanagement\PDC\01_WIP\01_Algemeen\X_000002_DatakwaliteitBaseline\Completness_Output_JSON"

def total_percentage_trues(df):
    '''
    This function returns the ratio between cells with the value 'True' and cells with outher values
    
    :param df: Dataframe for which the ratio needs to be calculated
    '''
    total_trues = df.sum().sum().compute()

    nr_rows = df.shape[0].compute()
    nr_cols = len(df.columns)
    total_values = nr_rows * nr_cols

    true_ratio = float(total_trues / total_values)
    return true_ratio

def return_percentage_values_column(df, column):
    '''
    This function returns a dictionary where the percentages of the occorunce of the values True and False are given
    
    :param df: The dataframe which contains the column
    :param column: The column for which to calculate the percentages
    '''
    sum_boolean = df[column].value_counts(dropna=False, split_out=8).compute()
    counts_dict = sum_boolean.to_dict()

    freq_series = sum_boolean / sum_boolean.sum()
    freq_dict = freq_series.to_dict()

    return freq_dict

#Percentage True/False per collumn

#let op! de trues zijn dus null-waarden
def return_percentage_true_column_df(df):
    '''
    This function returns a dictionary with as key the column name and as value a dictionary from the function return_percentage_values_column
    
    :param df: The dataframe for which to calculate the Trues and Falses per column
    '''

    column_dict = {}
    for column in df:
        if column != 'OBJECTID':  # OBJECTID overslaan
            column_dict[column] = return_percentage_values_column(df, column)

    return column_dict

def return_percentage_true_row(df):
    '''
    Function to return a dictionary which has the completeness per row
    
    :param df: The dataframe for which to calculate the Trues and Falses per row
    '''
    nr_columns = len(df.columns)
    sum_rows_series = (df == True).sum(axis=1).compute()
    sum_rows = sum_rows_series.to_dict()
    sum_rows_perc = {}

    for i in sum_rows:
        sum_rows_perc[i] = sum_rows[i]/nr_columns
    
    return sum_rows_perc



def return_percentage_true_row_OID(df):
    """
    Return {OBJECTID(str): percentage_true_per_row(float)} voor een Dask DataFrame
    met kolom 'OBJECTID' en verder alleen booleans. OBJECTID telt niet mee in percentage.
    """
    # True's per rij (OBJECTID niet meenemen)
    sum_rows_series = df.drop(columns=['OBJECTID']).eq(True).sum(axis=1).compute()
    
    # aantal kolommen (excl. OBJECTID)
    nr_columns = len(df.columns) - 1
    if nr_columns <= 0:
        return {}

    # Keys: forceer OBJECTID naar str zodat JSON altijd kan serialiseren
    objectids_str = df['OBJECTID'].astype(str).compute().tolist()

    # Values: native float (geen numpy types of Decimal)
    percentages = (sum_rows_series / float(nr_columns)).tolist()
    percentages = [float(p) for p in percentages]

    # Bouw dict
    return dict(zip(objectids_str, percentages))



def completeness_for_parquet(parquet):
    '''
    Docstring for completeness_for_parquet
    
    :param parquet: Description
    '''
    df = dd.read_parquet(parquet)
    df_actueel = df[df["GDB_TO_DATE"].isna()]
    df_boolean = df_actueel.isnull()

    #OBJECTID terugzetten
    df_boolean[["OBJECTID"]] = df_actueel[["OBJECTID"]]

    completeness = {}

    completeness["total_true_ratio"] = total_percentage_trues(df_boolean)

    completeness["column_true_ratio"] = return_percentage_true_column_df(df_boolean)

    completeness["nulls_per_row"] = return_percentage_true_row_OID(df_boolean)

    return completeness

def write_dictionary(json_name, dictionary):
    with open(json_name, 'w') as f:
        json.dump(dictionary, f)

def main():
    parquets = glob.glob("{}\*.parquet".format(parquet_folder))

    for parquet in parquets:
        try:
            print(parquet)
            parquet_name = os.path.split(parquet)[1]
            pq_completeness = completeness_for_parquet(parquet)
        
            json_naam = "{}_dict.json".format(parquet_name.removesuffix(".parquet"))

            os_naam_incpad = os.path.join(output_map, json_naam)
            print(os_naam_incpad)
            write_dictionary(os_naam_incpad, pq_completeness)
        except:
            print("Faalactie: naar kijken nog! {}".format(parquet))

if __name__ == "__main__":
    main()

    
