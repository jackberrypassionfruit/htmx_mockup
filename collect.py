import sys, os

project_dir = "/serialize/"
sys.path.append(project_dir)
os.environ["DJANGO_SETTINGS_MODULE"] = "my_project.settings"
# os.environ.setdefault("DJANGO_SETTINGS_MODULE", __file__)

import django

django.setup()

import polars as pl
from serialize.models import SerializeMaster
import sys

csv_file_path = sys.argv[1]
df = pl.read_csv(csv_file_path)

# Iterate through the DataFrame and create model instances
for row in df.iter_rows(named=True):
    # print(row)
    # Create the Product instance
    product = SerializeMaster(
        part_id=row["part_id"],
        configuration_id=row["configuration_id"],
        process_step_num=row["process_step_num"],
        job_number=row["job_number"],
        start_operator=row["start_operator"],
        start_datetime=row["start_datetime"]
        if row["start_datetime"] != "NULL"
        else None,
        end_operator=row["end_operator"],
        end_datetime=row["end_datetime"] if row["end_datetime"] != "NULL" else None,
        notes=row["notes"],
        scrapped=row["scrapped"],
        created_by=row["created_by"],
        created_datetime=row["created_datetime"]
        if row["created_datetime"] != "NULL"
        else None,
        modified_by=row["modified_by"],
        modified_datetime=row["modified_datetime"]
        if row["modified_datetime"] != "NULL"
        else None,
    )
    # to save the current product
    product.save()
    # print(product.start_datetime)

print("CSV data has been loaded into the Django database.")

# l = [
#     "part_id",
#     "configuration_id",
#     "process_step_num",
#     "job_number",
#     "start_operator",
#     "start_datetime",
#     "end_operator",
#     "end_datetime",
#     "notes",
#     "scrapped",
#     "created_by",
#     "created_datetime",
#     "modified_by",
#     "modified_datetime",
# ]

# for w in l:
#     print(f"{w} = row['{w}'],")
