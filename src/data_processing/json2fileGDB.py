# -------------------------------------------------------------------------------------------------
# Gemaakt door: Maarten Baas, Rens Spierings
# Datum laatste aanpassing 24-12-2025
# Dit Python script is gemaakt voor het verwerken van de compleetheid-jsons tot een feature class in de file geodatabase.
# het script is onderdeel van de eindopdracht voor de opleiding Data and AI Engineering van de EQI
# -------------------------------------------------------------------------------------------------

# import modules
import json
import arcpy
import os
import glob

#input_parameters:
json_file_folder = r"K:\CentraalDatamanagement\PDC\01_WIP\01_Algemeen\X_000002_DatakwaliteitBaseline\70_Resultaten\Completness_Output_JSON"
output_file_gdb = r"K:\CentraalDatamanagement\PDC\01_WIP\01_Algemeen\X_000002_DatakwaliteitBaseline\70_Resultaten\Completness_Output_JSON\Completeness.gdb"
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
    value_field="Completeness"
    value_field_type="DOUBLE"

    arcpy.management.AddField(output_fc, value_field, value_field_type)

    return output_fc


def fill_feature_class(input_sde, input_fc, output_fc, json_file):
    '''
    Functie om de aangemaakte feature class te vullen met de waarden uit de json_file
    
    :param input_sde: De input SDE, database connectie waar we de objectids en de geometrie uithalen 
    :param input_fc: De feature class waar we de shape uithalen
    :param output_fc: De te vullen feature class
    :param json_file: de input json_file uit het Features_Completeness_dask.py
    '''
    with open(json_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    with arcpy.da.SearchCursor(input_fc, ['OID@', "SHAPE@"]) as scur,\
         arcpy.da.InsertCursor(output_fc, ["SHAPE@", "Completeness"]) as icur:
        for row in scur:
            if str(row[0]) in data["nulls_per_row"]:
                icur.insertRow([row[1], data["nulls_per_row"][str(row[0])]])
            else:
                icur.insertRow([row[1], None])

def main():
    jsons = glob.glob("{}\*.json".format(json_file_folder))
                      

    for json_file in jsons:
        feature_class_name = get_fc_name(json_file)
        input_fc = "DAMO_W.{}".format(feature_class_name)


        try:
            output_fc = create_feature_class(input_sde, input_fc, output_file_gdb, feature_class_name)
            fill_feature_class(input_sde, input_fc, output_fc, json_file)    
            print("{} verwerkt".format(feature_class_name))   
        except Exception as e:
            print("Faalactie: naar kijken nog! {}".format(feature_class_name))
            print(e)

if __name__ == "__main__":
    main()