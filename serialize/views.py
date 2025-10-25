from django.shortcuts import render

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


def index(request):
    return render(
        request,
        "base/index.html",
        context={"jobs": jobs},
    )
