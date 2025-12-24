
import pandas as pd
import datetime

def process_history_table(input_file, output_file,
                          id_col='OBJECTID',
                          date_col='GDB_FROM_DATE',
                          sep=';', file_type='csv'):
    """
    Verwerkt een historie-tabel en maakt per ID één regel met:
    - Laatste waarde per attribuut
    - Oudste datum waarop die laatste waarde voorkomt
    Gebruikt alleen GDB_FROM_DATE als datumkolom.

    Parameters:
    ----------
    input_file : str
        Pad naar inputbestand (CSV of Parquet)
    output_file : str
        Pad naar outputbestand (Excel)
    id_col : str
        Kolom die het object identificeert
    date_col : str
        Datumkolom (standaard: GDB_FROM_DATE)
    sep : str
        Scheidingsteken voor CSV (default = ';')
    file_type : str
        'csv' of 'parquet'
    """

    # 1. Lees bestand in
    if file_type == 'csv':
        df = pd.read_csv(input_file, sep=',', dtype=str)
    elif file_type == 'parquet':
        df = pd.read_parquet(input_file)
    else:
        raise ValueError("file_type moet 'csv' of 'parquet' zijn.")

    # 2. Zet datumkolom om naar datetime
    df[date_col] = pd.to_datetime(df[date_col], errors='coerce')

    # 3. Detecteer attributen (alle kolommen behalve ID en datumkolom)
    exclude_cols = [id_col, date_col]
    value_cols = [c for c in df.columns if c not in exclude_cols]

    # 4. Unpivot naar lange vorm
    melted = df.melt(id_vars=[id_col, date_col], value_vars=value_cols,
                     var_name='attribute', value_name='value')

    # 5. Filter lege waarden
    #melted = melted.dropna(subset=['value'])

    # 6. Sorteer op datum
    melted = melted.sort_values([id_col, 'attribute', date_col])

    # 7. Voor elke OBJECTID + attribuut: laatste waarde + oudste datum voor die waarde
    def get_last_with_oldest_date(group):
        last_value = group.iloc[-1]['value']
        if pd.isna(last_value) or last_value == '':
            # Als laatste waarde NULL of leeg is → pak eerste datum in de groep
            oldest_date = group[date_col].min()
        else:
            same_value_rows = group[group['value'] == last_value]
            oldest_date = same_value_rows[date_col].min()
        return pd.Series({'last_value': last_value, 'last_change': oldest_date})

    result = melted.groupby([id_col, 'attribute']).apply(get_last_with_oldest_date).reset_index()

    # 8. Pivot terug naar breed formaat
    pivot_value = result.pivot(index=id_col, columns='attribute', values='last_value')
    pivot_date = result.pivot(index=id_col, columns='attribute', values='last_change')

    # 9. Kolomnamen aanpassen
    pivot_value.columns = [f"{col}_last_value" for col in pivot_value.columns]
    pivot_date.columns = [f"{col}_last_change" for col in pivot_date.columns]

    final_result = pd.concat([pivot_value, pivot_date], axis=1).reset_index()

    # 10. Maak datums timezone-vrij
    for col in final_result.columns:
        if 'change' in col:
            final_result[col] = pd.to_datetime(final_result[col], errors='coerce').dt.tz_localize(None)

    # 11. Schrijf naar Excel
    final_result.to_csv(output_file, index=False, sep=';')
    print(f"✅ Resultaat opgeslagen in: {output_file}")


# Voorbeeld aanroep:
# process_history_table("Brug_Test.csv", "Brug_last_changes.xlsx", file_type='csv')
# process_history_table("historie.parquet", "historie_last_changes.xlsx", file_type='parquet')

proces_started = datetime.datetime.now()
print('Process started at: {}'.format(proces_started))

process_history_table(r"K:\CentraalDatamanagement\PDC\01_WIP\01_Algemeen\X_000002_Datakwaliteit_Baseline\DAMO_H_CSV\DAMO_W.Brug_H.csv", r'C:\Users\RESP\Documents\Bronbestanden\DAMO_H_CSV\Brug_last_changes_20251128.csv', file_type='csv')

proces_ended = datetime.datetime.now()
print('Process ended at: {}'.format(proces_ended))

proces_duration = proces_ended - proces_started
print('Process duration: {}'.format(proces_duration))