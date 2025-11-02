import polars as pl


def get_scheduled_prints_df(in_file_path):
    today_prints = pl.read_csv(in_file_path, separator=",")

    today_prints_clean = today_prints.filter(
        pl.col("scrapped").str.starts_with("No")
    ).select(
        "request_type",
        "part_number",
        "platform",
        "assigned_printer",
        "printer_hood",
        "printer_model",
        "job_number",
        "print_number",
        "qty_parts",
        "plan_print_start_datetime",
        "estimated_print_time_minutes",
        # 'scrapped'
    )

    return today_prints_clean
