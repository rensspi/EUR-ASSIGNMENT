# -------------------------------------------------------------------------------------------------
# Gemaakt door: Maarten Baas, Rens Spierings
# Datum laatste aanpassing 24-12-2025
# Dit Python script is gemaakt voor het verwerken van de compleetheid-jsons tot een feature class in de file geodatabase.
# het script is onderdeel van de eindopdracht voor de opleiding Data and AI Engineering van de EQI
# -------------------------------------------------------------------------------------------------

# import modules
import csv
import arcpy
import os
import glob
import datetime

#input_parameters:
csv_file_folder = r"L:\Project9\TDAM Tijdelijke Opslag\Maarten\csvs"
output_file_gdb = r"L:\Project9\TDAM Tijdelijke Opslag\Maarten\actualiteit.gdb"
input_sde = r"L:\Project9\FME\FME_Centraal\Connecties\ESRI\DAMOPRD_raadpleger.sde"

def get_fc_name(input_json):
    '''
    Functie om de input json te strippen van het pad, de prefix en de suffix
    
    :param input_json: input json inclusief het pad, de prefix en de suffix
    '''
    file_name = os.path.split(input_json)[1]
    file_name_stripped = file_name.split('.')[1]
    file_name_strippeter = file_name_stripped.removesuffix('_H_dict')
    return file_name_strippeter


def create_feature_class(input_sde, input_fc, output_file_gdb, feature_class_name):
    '''
    Functie om een output feature class te maken in de opgegeven output file geodatabase
    
    :param input_sde: De input SDE, database connectie waar we de objectids en de geometrie uithalen
    :param input_fc: De feature class die als template voor de aan te maken feature class functioneert
    :param output_file_gdb: De output file geodatabse waarine we de feature class maken
    :param feature_class_name: De naam van de aan te maken feature class
    '''
    output_fc = os.path.join(output_file_gdb, feature_class_name)

    arcpy.env.workspace = input_sde

    desc = arcpy.Describe(input_fc)
    geom_type = desc.shapeType          # 'Point', 'Polyline', 'Polygon'
    sr = desc.spatialReference

    arcpy.management.CreateFeatureclass(output_file_gdb, feature_class_name, geom_type, spatial_reference=sr)

    #Dit staat er nu hard coded in, eventueel parameters van maken
    fields = arcpy.ListFields(input_fc)
    output_fields = []

    for field in fields:
        if field.name != "OBJECTID":
            new_field = "{}_act".format(field.name)
            arcpy.management.AddField(output_fc, new_field, "DATE")
            output_fields.append(new_field)

    return output_fc, output_fields

def read_csv_to_dict(csv_file, DATE_FORMAT):
    with open(path, encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        return {
            row["OBJECTID"]: {
                k: datetime.strptime(v, DATE_FORMAT) if v.strip() else None
                for k, v in row.items() if k != "OBJECTID"
            }
            for row in reader
        }


def fill_feature_class(input_sde, input_fc, output_fc, csv_file, output_fields):
    '''
    Functie om de aangemaakte feature class te vullen met de waarden uit de json_file
    
    :param input_sde: De input SDE, database connectie waar we de objectids en de geometrie uithalen 
    :param input_fc: De feature class waar we de shape uithalen
    :param output_fc: De te vullen feature class
    :param json_file: de input json_file uit het Features_Completeness_dask.py
    '''
    DATE_FORMAT = "%Y-%m-%d %H:%M:%S"  # jouw formaat
    data = read_csv_to_dict(csv_file)
    
    icur_fields = ["SHAPE@"] + output_fields

    with arcpy.da.SearchCursor(input_fc, ['OID@', "SHAPE@"]) as scur,\
         arcpy.da.InsertCursor(output_fc, icur_fields) as icur:
        for row in scur:
            if str(row[0]) in data:
                row_filled = []
                for field in icur_fields:
                    field_name = "{}_last_changed".format(field.removesuffix("_act"))

                    row_filled.append(data[str(row[0])][field_name])
                icur.insertRow(row_filled)
#            else:
#                icur.insertRow([row[1], None])

def main():
    csvs = glob.glob("{}\*.csv".format(csv_file_folder))
                      

    for csv_file in csvs:
        feature_class_name = get_fc_name(csv_file)
        print("gestart met {}".format(feature_class_name))
        input_fc = "DAMO_W.{}".format(feature_class_name)

        try:
            output_fc, output_fields = create_feature_class(input_sde, input_fc, output_file_gdb, feature_class_name)
            fill_feature_class(input_sde, input_fc, output_fc, csv_file, output_fields)    
            print("{} verwerkt".format(feature_class_name))   
        except Exception as e:
            print("Faalactie: naar kijken nog! {}".format(feature_class_name))
            print(e)

if __name__ == "__main__":
    main()