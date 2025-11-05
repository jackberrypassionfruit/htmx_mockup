import polars as pl


def get_scheduled_prints_df(in_file_path):
    schema_dict = pl.Schema(
        {
            "order_id": str,
            "part_number": str,
            "request_type": str,
            "die_revision": str,
            "configuration_id": int,
            "platform": str,
            "assigned_printer": str,
            "printer_hood": str,
            "printer_model": str,
            "completion_date": pl.Date,
            "job_number": str,
            "print_number": int,
            "qty_parts": int,
            "plan_print_start_datetime": str,
            "print_file": str,
            "print_file_name_cfg": str,
            "material_file_name_cfg": str,
            "estimated_print_time": str,
            "estimated_print_time_minutes": int,
            "master_job_map_pk_id": int,
            "estimated_plan_print_end_datetime": str,
            "actual_print_start_datetime": str,
            "actual_print_end_datetime": str,
            "scrapped": str,
            "duplicate_estimated_end_times": str,
        }
    )
    today_prints = pl.read_csv(in_file_path, schema=schema_dict, separator=",")
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


def get_active_printers_df(in_file_path):
    schema_dict = pl.Schema(
        {
            "equipment_id": str,
            "common_name": str,
            "model_number": str,
            "serial_number": str,
            "condition": str,
            "printer_hood": str,
        }
    )
    active_printers = pl.read_csv(in_file_path, schema=schema_dict, separator=",")
    active_printers_clean = active_printers

    return active_printers_clean
