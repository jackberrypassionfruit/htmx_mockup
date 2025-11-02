from django.shortcuts import render, redirect
from django.http import HttpResponse
from .etl.extract import get_scheduled_prints_df
from .etl.transform import partition_prints_by_printer_ordered
import polars as pl
import os

# Create your views here.

in_file_path = os.path.join(".", "swimlane", "test_files", "schedule_swimlane_now.csv")

today_prints = get_scheduled_prints_df(in_file_path)

today_prints = today_prints.with_columns(
    pl.concat_str(
        [
            pl.lit("calc("),
            pl.col("estimated_print_time_minutes") / 17.5,
            pl.lit("% - 20px)"),
        ],
        separator="",
    ).alias("x_coord_style")
)

prints_by_printer = partition_prints_by_printer_ordered(today_prints)


def schedule(request):
    if request.method == "GET":
        clock_hours = ["".join(list("0" + str((i - 2) % 24))[-2:]) for i in range(29)]
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
