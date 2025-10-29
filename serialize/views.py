from django.shortcuts import render
from django.http import HttpResponse

from .models import SerializeMaster
from django.db.models import Count, Q

# Create your views here.

jobs = [
    {
        "job_number": "6238749",
        "part_number": "9341028",
        "qty_parts": 21,
        "request_type": "Eng Trial",
    },
    {
        "job_number": "6238746",
        "part_number": "9341028",
        "qty_parts": 21,
        "request_type": "Eng Trial",
    },
    {
        "job_number": "6238744",
        "part_number": "9341028",
        "qty_parts": 18,
        "request_type": "Production",
    },
    {
        "job_number": "6238743",
        "part_number": "9341028",
        "qty_parts": 19,
        "request_type": "Production",
    },
]

parts = [
    "12899-125A",
    "27395-893R",
    "12315-183L",
    "18945-121H",
    "12749-123Y",
]


def index(request):
    not_scrapped_by_job = (
        SerializeMaster.objects.filter(Q(scrapped="NULL"))
        .values("job_number")
        .annotate(qty_parts=Count("part_id"))
    )
    return render(
        request,
        "base/index.html",
        # "testing/modal.html",
        context={"jobs": not_scrapped_by_job},
    )


def get_parts(request, job_number):
    parts_filtered_by_job = SerializeMaster.objects.filter(
        Q(job_number=job_number)
    ).values("part_id")
    return render(
        request,
        "partial/parts.html",
        context={"parts": parts_filtered_by_job},
    )
    # return HttpResponse("fart")
