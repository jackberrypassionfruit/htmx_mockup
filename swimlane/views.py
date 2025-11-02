from django.shortcuts import render, redirect
from django.http import HttpResponse
from .etl.extract import get_scheduled_prints_df
from .etl.transform import partition_prints_by_printer_ordered_w_style
import polars as pl
import os

# Create your views here.

in_file_path = os.path.join(".", "swimlane", "test_files", "schedule_swimlane_now.csv")
selected_date = "2025-10-31 00:00:00"

today_prints = get_scheduled_prints_df(in_file_path)
prints_by_printer = partition_prints_by_printer_ordered_w_style(
    today_prints, selected_date
)


def schedule(request):
    if request.method == "GET":
        clock_hours = [
            {
                "hour": ("0" + str((n - 2) % 24))[-2:],
                "offset": ((n + 2.5) * 3.25),  # TODO make good
            }
            for n in range(29)
        ]
        context = {"clock_hours": clock_hours, "prints_by_printer": prints_by_printer}
        return render(
            request,
            "swimlane/index.html",
            # "testing/modal.html",
            context=context,
        )


def style_test(request):
    if request.method == "GET":
        return HttpResponse('<p style="font-size:30px;">this is a test</p>')
