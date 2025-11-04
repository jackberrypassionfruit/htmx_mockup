from django.shortcuts import render, redirect
from django.http import HttpResponse
from .etl.extract import get_scheduled_prints_df
from .etl.transform import (
    partition_prints_by_printer_ordered_w_style,
    filter_and_cache_prints,
)
from datetime import timedelta
from dateutil import parser
import os

# Create your views here.

clock_hours = [
    {
        "index": n - 2,
        "hour": ("0" + str((n - 2) % 24))[-2:],
        "offset": ((n + 2.75) * 3.23),
    }
    for n in range(29)
]

in_file_path = os.path.join(".", "swimlane", "test_files", "schedule_swimlane_now.csv")
selected_date = parser.parse("2025-10-31")


def refresh_prints(selected_date):
    today_prints = get_scheduled_prints_df(in_file_path)
    return today_prints


def repaint_day(selected_prints, selected_date):
    prints_by_printer = partition_prints_by_printer_ordered_w_style(
        selected_prints, selected_date
    )

    return prints_by_printer


today_prints = refresh_prints(selected_date)
prints_by_printer = repaint_day(today_prints, selected_date)


def schedule(request):
    if request.method == "GET":
        context = {
            "clock_hours": clock_hours,
            "prints_by_printer": prints_by_printer,
        }
        return render(
            request,
            "swimlane/index.html",
            context=context,
        )


def refresh(request):
    global today_prints
    global cached_prints
    global prints_by_printer
    if request.method == "GET":
        context = {"clock_hours": clock_hours}
        if request.headers.get("HX-Trigger") == "schedule-datepicker":
            # context = dict(request.GET)
            # context = {key: val[0] for key, val in context.items()}
            # print(f"{context=}")

            selected_date = parser.parse(request.GET.get("selected_date"))
            today_prints = refresh_prints(selected_date)
            prints_by_printer = repaint_day(today_prints, selected_date)
        elif request.headers.get("HX-Trigger") == "schedule-cache":
            selected_date = parser.parse(request.GET.get("selected_date"))
            selected_hour_index = int(request.GET.get("selected_hour_index"))
            selected_datetime = selected_date + timedelta(hours=selected_hour_index)

            (today_prints, cached_prints) = filter_and_cache_prints(
                today_prints, selected_datetime
            )
            print(f"{cached_prints=}")

            prints_by_printer = repaint_day(today_prints, selected_date)
        elif request.headers.get("HX-Trigger") == "reset-schedule":
            selected_date = parser.parse(request.GET.get("selected_date"))
            today_prints = refresh_prints(selected_date)
            prints_by_printer = repaint_day(today_prints, selected_date)

        context["prints_by_printer"] = prints_by_printer
        return render(
            request,
            "swimlane/content/gantt.html",
            context=context,
        )


def style_test(request):
    if request.method == "GET":
        return HttpResponse('<p style="font-size:30px;">this is a test</p>')
