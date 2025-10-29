import sys, os

project_dir = "/serialize/"
sys.path.append(project_dir)
os.environ["DJANGO_SETTINGS_MODULE"] = "my_project.settings"
# os.environ.setdefault("DJANGO_SETTINGS_MODULE", __file__)

import django
from django.db.models import Count, Q

django.setup()

from serialize.models import SerializeMaster
import sys

# total_parts = SerializeMaster.objects.count()
# print(f"{total_parts=}")

# total_not_scrapped = (
#     SerializeMaster.objects.filter(Q(scrapped="NULL")).values().count()
#     # SerializeMaster.objects.filter(scrapped__exact="NULL").values().count()
# )
# print(f"{total_not_scrapped=}")

# not_scrapped_by_job = (
#     SerializeMaster.objects.filter(Q(scrapped="NULL"))
#     .values("job_number")
#     .annotate(qty_parts=Count("part_id"))
#     # SerializeMaster.objects.filter(scrapped__exact="NULL").values().count()
# )
# print(f"{not_scrapped_by_job=}")

parts_filtered_by_job = SerializeMaster.objects.filter(Q(job_number="6238714")).values(
    "part_id"
)
print(f"{parts_filtered_by_job=}")
