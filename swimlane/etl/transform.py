from .extract import schema_dict
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
                    "debug": print["plan_print_start_datetime"][-8:],
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
    ).total_seconds() / 60 > minimum_gap_between_prints_minutes:
        initial_production_start_time = parser.parse(newest_prod_print)
    else:
        initial_production_start_time = selected_start_time - timedelta(
            minutes=minimum_gap_between_prints_minutes
        )

    # return initial_production_start_time

    iter_count = 0
    fail_count = 0
    confirmed_scheduled_prints = pl.DataFrame()

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
                    (
                        None
                        if current_schedule_attempt.is_empty()
                        else parser.parse(
                            current_schedule_attempt.filter(
                                pl.col("job_number") != pl.lit(job_number)
                            )
                            .select(pl.max("plan_print_start_datetime"))
                            .item()
                        )
                    )
                    or (
                        None
                        if confirmed_scheduled_prints.is_empty()
                        else parser.parse(
                            confirmed_scheduled_prints.filter(
                                pl.col("job_number") != pl.lit(job_number)
                            )
                            .select(pl.max("plan_print_start_datetime"))
                            .item()
                        )
                    )
                    or initial_production_start_time
                ) + timedelta(minutes=fail_count * minimum_gap_between_prints_minutes)

                new_print_start_time = latest_scheduled_start_other_job + timedelta(
                    minutes=minimum_gap_between_prints_minutes
                )

                new_print_end_time = new_print_start_time + timedelta(
                    minutes=this_job_estimated_print_time_minutes
                )

                potential_overlapping_prints = scheduled_prints.filter(
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
                    next_print_attempt = print_this_job
                    next_print_attempt["plan_print_start_datetime"] = (
                        new_print_start_time
                    )
                    next_print_attempt["estimated_plan_print_end_datetime"] = (
                        new_print_end_time
                    )
                    next_print_attempt["printer_hood"] = ""

                    avail_printers = []
                    for printer in eligible_printers:
                        # 1 check if this printers' last print will be finished
                        # by the time this next print is to schedule
                        last_end_time_current_schedule_attempt = (
                            None
                            if current_schedule_attempt.is_empty()
                            else (
                                (
                                    current_schedule_attempt.filter(
                                        pl.col("assigned_printer")
                                        == pl.lit(printer["equipment_id"])
                                    )
                                )
                                .select(pl.max("estimated_plan_print_end_datetime"))
                                .item()
                            )
                        )
                        last_end_time_confirmed_scheduled_prints = (
                            None
                            if confirmed_scheduled_prints.is_empty()
                            else (
                                confirmed_scheduled_prints.filter(
                                    pl.col("assigned_printer")
                                    == pl.lit(printer["equipment_id"])
                                )
                            )
                            .select(pl.max("estimated_plan_print_end_datetime"))
                            .item()
                        )

                        is_this_printer_finished_with_last_print = (
                            last_end_time_current_schedule_attempt is None
                            and last_end_time_confirmed_scheduled_prints is None
                        ) or (
                            parser.parse(
                                last_end_time_current_schedule_attempt
                                or last_end_time_confirmed_scheduled_prints
                            )
                            <= latest_scheduled_start_other_job
                        )

                        # 2 make sure this next print does not overlap
                        # with any "potential_overlapping_prints"
                        noverlaps_w_this_round = (
                            potential_overlapping_prints.filter(
                                pl.col("assigned_printer")
                                == pl.lit(print_this_job["assigned_printer"]),
                                new_print_start_time
                                < pl.col(
                                    "estimated_plan_print_end_datetime"
                                ).str.to_datetime(datetime_fmt)
                                + pl.duration(
                                    minutes=minimum_gap_between_prints_minutes
                                ),
                                pl.col("plan_print_start_datetime").str.to_datetime(
                                    datetime_fmt
                                )
                                - pl.duration(
                                    minutes=minimum_gap_between_prints_minutes
                                )
                                <= new_print_end_time,
                            )
                            .select(pl.count())
                            .item()
                        ) == 0

                        # 3 Make sure new print start time doesn't come within gap_time
                        # of any pre-existing prints

                        # TODO merge this with above

                        # 4 make sure this job is not already being printed on this printer
                        noverlaps_w_this_job = (
                            True
                            if current_schedule_attempt.is_empty()
                            else (
                                current_schedule_attempt.filter(
                                    pl.col("job_number") == pl.lit(job_number),
                                    pl.col("assigned_printer")
                                    == pl.lit(printer["equipment_id"]),
                                )
                                .select(pl.count())
                                .item()
                            )
                            == 0
                        )

                        if (
                            is_this_printer_finished_with_last_print
                            and noverlaps_w_this_round
                            and noverlaps_w_this_job
                        ):
                            avail_printers.append(printer)

                        if len(avail_printers) >= 1:
                            next_print_attempt["assigned_printer"] = sorted(
                                avail_printers,
                                key=lambda x: (x["printer_hood"], x["equipment_id"]),
                                reverse=False,
                            )[0]["equipment_id"]
                        else:
                            next_print_attempt["assigned_printer"] = ""

                    current_schedule_attempt = pl.concat(
                        [
                            current_schedule_attempt,
                            pl.from_dict(
                                next_print_attempt, schema=schema_dict, strict=False
                            ),
                        ]
                    )

            # fix printer_hood to match assigned_printer
            current_schedule_attempt = current_schedule_attempt.update(
                active_printers.select("equipment_id", "printer_hood"),
                left_on=["assigned_printer"],
                right_on=["equipment_id"],
                how="inner",
            )

            confirmed_scheduled_prints = pl.concat(
                [confirmed_scheduled_prints, current_schedule_attempt]
            )

    # print(
    #     confirmed_scheduled_prints.select(
    #         "job_number",
    #         # "print_number",
    #         # "plan_print_start_datetime",
    #         # "estimated_plan_print_end_datetime",
    #         # "estimated_print_time_minutes",
    #     )
    # )
    return confirmed_scheduled_prints
