#PythonCaller:
import fme
from fme import BaseTransformer
import fmeobjects
from collections import defaultdict
from datetime import datetime
import csv
import os

class FeatureProcessor(BaseTransformer):
    def __init__(self):
        self.feature_groups = defaultdict(list)
        self.output_folder = fme.macroValues["output_csv_folder"]  # Retrieve the user parameter for the output folder

    def input(self, feature: fmeobjects.FMEFeature):
        object_id = feature.getAttribute("OBJECTID")
        self.feature_groups[object_id].append(feature)

    def close(self):
        for object_id, features in self.feature_groups.items():
            # Sort features by GDB_FROM_DATE in ascending order
            features.sort(key=lambda f: datetime.strptime(f.getAttribute("GDB_FROM_DATE"), "%Y-%m-%d %H:%M:%S"))

            # Get the feature with the latest GDB_FROM_DATE
            latest_feature = features[-1]

            # Create a dictionary to store the earliest GDB_FROM_DATE for each attribute
            last_changed_dates = {}

            for feature in features:
                for attr in feature.getAllAttributeNames():
                    if attr not in ["OBJECTID", "GDB_FROM_DATE"]:
                        if feature.getAttribute(attr) == latest_feature.getAttribute(attr):
                            if attr not in last_changed_dates:
                                last_changed_dates[attr] = feature.getAttribute("GDB_FROM_DATE")

            # Add the new attributes to the latest feature
            for attr, date in last_changed_dates.items():
                latest_feature.setAttribute(f"{attr}_last_changed", date)

            # Determine the basename for the CSV file
            basename = latest_feature.getAttribute("fme_basename")
            if not basename:
                basename = "default"

            # Prepare the CSV file path in the specified output folder
            csv_file_path = os.path.join(self.output_folder, f"{basename}.csv")

            # Ensure the output folder exists
            os.makedirs(self.output_folder, exist_ok=True)

            # Write the attributes including OBJECTID and those with the suffix "_last_changed" to the CSV file
            with open(csv_file_path, mode="a", newline="", encoding="utf-8") as csv_file:
                writer = csv.writer(csv_file)

                # Include OBJECTID and filter attributes with the suffix "_last_changed"
                last_changed_attrs = ["OBJECTID"] + [attr for attr in latest_feature.getAllAttributeNames() if attr.endswith("_last_changed")]

                # Write header if the file is empty
                if os.stat(csv_file_path).st_size == 0:
                    writer.writerow(last_changed_attrs)

                # Write the feature attributes
                row = [latest_feature.getAttribute(attr) for attr in last_changed_attrs]
                writer.writerow(row)

            # Output the latest feature with the new attributes
            self.pyoutput(latest_feature, output_tag="PYOUTPUT")
