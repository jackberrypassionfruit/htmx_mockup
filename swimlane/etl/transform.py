import polars as pl
from dateutil import parser
from datetime import timedelta

datetime_fmt = "%Y-%m-%d %H:%M:%S"


def partition_prints_by_printer_ordered_w_style(today_prints_clean, selected_date):
    prints_by_printer = [
        (
            str.strip(row.select(pl.first("assigned_printer")).item())[-3:],
            ("0" + str.strip(row.select(pl.first("printer_hood")).item()))[-2:],
            str.strip(row.select(pl.first("printer_model")).item()),
            [
                print
                | {
                    "prindex": prindex,
                    "print_width_style": f"calc({print['estimated_print_time_minutes'] / 17.5}% - 20px)",
                    "x_coord_style": str(
                        (
                            0.85
                            * (
                                parser.parse(print["plan_print_start_datetime"])
                                - selected_date
                            ).total_seconds()
                            / 60
                            # / 2.9
                            # - (prindex * print["estimated_print_time_minutes"])
                        )
                        + 160
                    )
                    + "px",
                    # "debug":  # print["plan_print_start_datetime"][-8:],
                    # (
                    #     parser.parse(print["plan_print_start_datetime"])
                    #     - selected_date
                    # ).total_seconds(),
                }
                for prindex, print in enumerate(row.to_dicts())
            ],
        )
        for row in today_prints_clean.sort(
            [
                "printer_hood",
                "assigned_printer",
                "plan_print_start_datetime",
            ]
        ).partition_by("assigned_printer")
    ]

    return prints_by_printer


def filter_and_cache_prints(today_prints, selected_datetime):
    today_prints_filtered = today_prints.filter(
        # pl.col("request_type").str.starts_with("Production") |
        pl.col("printer_hood").str.starts_with("M")
        | (
            pl.col("plan_print_start_datetime").str.to_datetime(datetime_fmt)
            < pl.lit(selected_datetime)
        ),
    )

    cached_prints = today_prints.filter(
        # ~pl.col("request_type").str.starts_with("Production"),
        ~pl.col("printer_hood").str.starts_with("M"),
        pl.col("plan_print_start_datetime").str.to_datetime(datetime_fmt)
        >= pl.lit(selected_datetime),
    )

    return (today_prints_filtered, cached_prints)


def schedule_cached_prints(
    minimum_gap_between_prints_minutes,
    selected_start_time,
    active_printers,
    scheduled_prints,
    cached_prints,
):
    eligible_printers = active_printers.filter(
        ~pl.col("printer_hood").str.starts_with("M")
    ).to_dicts()

    newest_prod_print = (
        scheduled_prints.filter(~pl.col("printer_hood").str.starts_with("M"))
        .select(pl.max("plan_print_start_datetime"))
        .item()
    )

    if not newest_prod_print:
        initial_production_start_time = selected_start_time - timedelta(
            minutes=minimum_gap_between_prints_minutes
        )
    elif (
        parser.parse(newest_prod_print) - selected_start_time
    ) > minimum_gap_between_prints_minutes:
        initial_production_start_time = newest_prod_print
    else:
        initial_production_start_time = selected_start_time - timedelta(
            minutes=minimum_gap_between_prints_minutes
        )

    # return initial_production_start_time

    iter_count = 0
    fail_count = 0
    confirmed_scheduled_prints = []

    test_l = []

    for i in range(1):
        current_schedule_attempt = pl.DataFrame()
        if len(cached_prints) > 0:
            pre_clean_unscheduled_prints = pl.DataFrame()
            iter_count += 1

            for job_number in pl.Series(
                cached_prints.unique(subset="job_number").select("job_number")
            ).to_list():
                this_job_estimated_print_time_minutes = (
                    cached_prints.filter(pl.col("job_number") == pl.lit(job_number))
                    .select(pl.first("estimated_print_time_minutes"))
                    .item()
                )

                latest_scheduled_start_other_job = (
                    # current_schedule_attempt.filter(
                    #     pl.col("job_number") != pl.lit(job_number)
                    # )
                    # .max("plan_print_start_datetime")
                    # .item()
                    # or confirmed_scheduled_prints.filter(
                    #     pl.col("job_number") != pl.lit(job_number)
                    # )
                    # .max("plan_print_start_datetime")
                    # .item()
                    # or
                    initial_production_start_time
                ) + timedelta(minutes=fail_count * minimum_gap_between_prints_minutes)

                # test_l.append(latest_scheduled_start_other_job)

                new_print_start_time = latest_scheduled_start_other_job + timedelta(
                    minutes=fail_count * minimum_gap_between_prints_minutes
                )

                new_print_end_time = new_print_start_time + timedelta(
                    minutes=this_job_estimated_print_time_minutes
                )

                potential_overlapping_prints = cached_prints.filter(
                    pl.col("plan_print_start_datetime").str.to_datetime(datetime_fmt)
                    >= (
                        new_print_start_time
                        - timedelta(
                            minutes=minimum_gap_between_prints_minutes
                            + this_job_estimated_print_time_minutes
                        )
                    ),
                    pl.col("plan_print_start_datetime").str.to_datetime(datetime_fmt)
                    <= (
                        new_print_start_time
                        + timedelta(
                            minutes=minimum_gap_between_prints_minutes
                            + this_job_estimated_print_time_minutes
                        )
                    ),
                )

                for print_this_job in cached_prints.filter(
                    pl.col("job_number") == pl.lit(job_number)
                ).to_dicts():
                    # print(print_this_job)
                    # TODO Leaving off at line 138: Collect(current_schedule_attempt)
                    next_print_attempt = {}

    return test_l
