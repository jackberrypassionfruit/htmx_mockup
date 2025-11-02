import polars as pl


def partition_prints_by_printer_ordered(today_prints_clean):
    prints_by_printer = [
        (
            str.strip(
                row.select(pl.first("assigned_printer")).to_dict(as_series=False)[
                    "assigned_printer"
                ][0]
            )[-3:],
            "".join(
                list(
                    "0"
                    + str.strip(
                        row.select(pl.first("printer_hood")).to_dict(as_series=False)[
                            "printer_hood"
                        ][0]
                    )
                )[-2:]
            ),
            str.strip(
                row.select(pl.first("printer_model")).to_dict(as_series=False)[
                    "printer_model"
                ][0]
            ),
            row.to_dicts(),
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
