import polars as pl
from dateutil import parser


def partition_prints_by_printer_ordered_w_style(today_prints_clean, selected_date):
    prints_by_printer = [
        (
            str.strip(
                row.select(pl.first("assigned_printer")).to_dict(as_series=False)[
                    "assigned_printer"
                ][0]
            )[-3:],
            (
                "0"
                + str.strip(
                    row.select(pl.first("printer_hood")).to_dict(as_series=False)[
                        "printer_hood"
                    ][0]
                )
            )[-2:],
            str.strip(
                row.select(pl.first("printer_model")).to_dict(as_series=False)[
                    "printer_model"
                ][0]
            ),
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
    today_prints = today_prints.filter(
        pl.col("plan_print_start_datetime").str.to_datetime("%Y-%m-%d %H:%M:%S")
        < pl.lit(selected_datetime)
    )

    cached_prints = today_prints.filter(
        pl.col("plan_print_start_datetime").str.to_datetime("%Y-%m-%d %H:%M:%S")
        >= pl.lit(selected_datetime)
    )

    return (today_prints, cached_prints)
