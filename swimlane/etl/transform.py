from .extract import schema_dict
import polars as pl
from dateutil import parser
from datetime import datetime, timedelta

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
                                datetime.strptime(
                                    print["plan_print_start_datetime"], datetime_fmt
                                )
                                - selected_date
                            ).total_seconds()
                            / 60
                            # / 2.9
                            # - (prindex * print["estimated_print_time_minutes"])
                        )
                        + 160
                    )
                    + "px",
                    "debug": print["job_number"],
                    # print["plan_print_start_datetime"][-8:],
                    # (
                    #     datetime.strptime(print["plan_print_start_datetime"], datetime_fmt)
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


def schedule_cached_prints_old(
    minimum_gap_between_prints_minutes,
    selected_start_time,
    active_printers,
    scheduled_prints,
    cached_prints,
):
    checkpoint_now = datetime.now()
    checkpoint_then = checkpoint_now
    checkpoint_start = checkpoint_now
    i = 1
    print(f"Checkpoint #{i=}")
    print(checkpoint_now - checkpoint_then, "\n\n")

    eligible_printers = sorted(
        active_printers.filter(~pl.col("printer_hood").str.starts_with("M")).to_dicts(),
        key=lambda x: (x["printer_hood"], x["equipment_id"]),
        reverse=False,
    )

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
        datetime.strptime(newest_prod_print, datetime_fmt) - selected_start_time
    ).total_seconds() / 60 > minimum_gap_between_prints_minutes:
        initial_production_start_time = datetime.strptime(
            newest_prod_print, datetime_fmt
        )
    else:
        initial_production_start_time = selected_start_time - timedelta(
            minutes=minimum_gap_between_prints_minutes
        )

    iter_count = 0
    fail_count = 0
    confirmed_scheduled_prints = pl.DataFrame()

    test_l = []

    for i in range(1):
        current_schedule_attempt = pl.DataFrame()
        if len(cached_prints) > 0:
            iter_count += 1

            for job_number in (
                pl.Series(
                    cached_prints.unique(subset="job_number").select("job_number")
                )
                .sort()
                .to_list()
            ):
                this_job_estimated_print_time_minutes = (
                    cached_prints.filter(pl.col("job_number") == pl.lit(job_number))
                    .select(pl.first("estimated_print_time_minutes"))
                    .item()
                )

                latest_scheduled_start_other_job = (
                    (
                        None
                        if current_schedule_attempt.is_empty()
                        else datetime.strptime(
                            current_schedule_attempt.filter(
                                pl.col("job_number") != pl.lit(job_number)
                            )
                            .select(pl.max("plan_print_start_datetime"))
                            .item(),
                            datetime_fmt,
                        )
                    )
                    or (
                        None
                        if confirmed_scheduled_prints.is_empty()
                        else datetime.strptime(
                            confirmed_scheduled_prints.filter(
                                pl.col("job_number") != pl.lit(job_number)
                            )
                            .select(pl.max("plan_print_start_datetime"))
                            .item(),
                            datetime_fmt,
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

                    next_print_attempt["assigned_printer"] = ""
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
                            datetime.strptime(
                                last_end_time_current_schedule_attempt
                                or last_end_time_confirmed_scheduled_prints,
                                datetime_fmt,
                            )
                            <= latest_scheduled_start_other_job
                        )

                        if not is_this_printer_finished_with_last_print:
                            continue

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

                        if not noverlaps_w_this_round:
                            continue

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

                        if not noverlaps_w_this_job:
                            continue

                        # If it passes all the test, chose this printer and break out of the for loop
                        next_print_attempt["assigned_printer"] = printer["equipment_id"]
                        break

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

    duration = datetime.now() - checkpoint_start
    print("Total print time:")
    print(duration, end="\n\n\n")

    # for p in eligible_printers:
    #     print(f'{p['printer_hood']=}, {p['equipment_id']=}')

    return confirmed_scheduled_prints


def schedule_cached_prints(
    minimum_gap_between_prints_minutes,
    selected_start_time,
    active_printers,
    scheduled_prints,
    cached_prints,
):
    """
    Refactored version of schedule_cached_prints.

    Key changes vs V1:
    - Polars dataframes are read once up front and converted to Python dicts/lists
      for all hot-path logic. No Polars operations inside the scheduling loops.
    - pl.concat inside the inner loop replaced with list accumulation + single
      pl.from_dicts() at the end.
    - Printer availability tracked as a plain dict keyed by equipment_id, value
      is a list of (start, end) datetime tuples representing already-scheduled
      intervals on that printer (from both committed history and the current
      scheduling attempt). This supports interior-gap detection, not just
      "last end time" tracking.
    - Constraint checks are broken into named helper functions for readability
      and so new constraints can be added without touching the loop structure.
    """

    # ------------------------------------------------------------------
    # 1. Pre-process inputs into Python-native structures (done once)
    # ------------------------------------------------------------------

    # Filter to non-M printers and sort, same as before
    eligible_printers = sorted(
        active_printers.filter(~pl.col("printer_hood").str.starts_with("M")).to_dicts(),
        key=lambda x: (x["printer_hood"], x["equipment_id"]),
    )
    eligible_printer_ids = [p["equipment_id"] for p in eligible_printers]

    # Build a lookup from equipment_id -> printer_hood for the fix-up step at the end
    printer_hood_by_id = {
        p["equipment_id"]: p["printer_hood"] for p in eligible_printers
    }

    # Determine initial production start time (same logic as before)
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
        datetime.strptime(newest_prod_print, datetime_fmt) - selected_start_time
    ).total_seconds() / 60 > minimum_gap_between_prints_minutes:
        initial_production_start_time = datetime.strptime(
            newest_prod_print, datetime_fmt
        )
    else:
        initial_production_start_time = selected_start_time - timedelta(
            minutes=minimum_gap_between_prints_minutes
        )

    # ------------------------------------------------------------------
    # 2. Build per-printer interval lists from already-scheduled prints
    #
    #    Each entry is a (start_dt, end_dt) tuple of datetime objects.
    #    Sorted ascending by start time.
    #    This is the core data structure that replaces repeated Polars
    #    filter calls inside the loop.
    # ------------------------------------------------------------------

    # Convert scheduled_prints to dicts once
    scheduled_prints_dicts = scheduled_prints.filter(
        ~pl.col("printer_hood").str.starts_with("M")
    ).to_dicts()

    # printer_intervals: dict[printer_id -> list of (start_dt, end_dt)]
    printer_intervals: dict[str, list[tuple[datetime, datetime]]] = {
        pid: [] for pid in eligible_printer_ids
    }
    for row in scheduled_prints_dicts:
        pid = row["assigned_printer"]
        if pid not in printer_intervals:
            continue
        start = datetime.strptime(row["plan_print_start_datetime"], datetime_fmt)
        end = datetime.strptime(row["estimated_plan_print_end_datetime"], datetime_fmt)
        printer_intervals[pid].append((start, end))

    for pid in printer_intervals:
        printer_intervals[pid].sort(key=lambda x: x[0])

    # ------------------------------------------------------------------
    # 3. Pre-process cached_prints into a plain list of dicts, grouped
    #    by job_number and sorted so we iterate in a stable order.
    # ------------------------------------------------------------------

    cached_dicts = cached_prints.to_dicts()

    # Unique sorted job numbers
    job_numbers = sorted(
        cached_prints.unique(subset="job_number")
        .select("job_number")
        .to_series()
        .to_list()
    )

    # Group cached prints by job_number
    prints_by_job: dict[str, list[dict]] = {jn: [] for jn in job_numbers}
    for row in cached_dicts:
        prints_by_job[row["job_number"]].append(row)

    # Duration per job (same for all prints in a job)
    duration_by_job: dict[str, float] = {
        jn: prints_by_job[jn][0]["estimated_print_time_minutes"] for jn in job_numbers
    }

    # ------------------------------------------------------------------
    # 4. Helper: find the latest scheduled start time across all jobs
    #    except the current one, from accumulated results so far.
    #    This replaces the two-stage Polars max() queries.
    # ------------------------------------------------------------------

    def latest_start_excluding_job(
        job_number: str,
        accumulated: list[dict],
        fail_count: int,
    ) -> datetime:
        candidates = [
            row["plan_print_start_datetime"]
            for row in accumulated
            if row["job_number"] != job_number
        ]
        if candidates:
            base = datetime.strptime(max(candidates), datetime_fmt)
        else:
            base = initial_production_start_time
        return base + timedelta(minutes=fail_count * minimum_gap_between_prints_minutes)

    # ------------------------------------------------------------------
    # 5. Helper: check whether a candidate (start, end) window overlaps
    #    any existing interval on a given printer, respecting the gap.
    #
    #    Two intervals [A_start, A_end] and [B_start, B_end] conflict if:
    #       A_start < B_end + gap  AND  B_start - gap < A_end
    #
    #    printer_intervals[pid] is already sorted, so we could binary
    #    search, but linear scan is fine for typical schedule sizes.
    # ------------------------------------------------------------------

    def printer_is_clear(
        pid: str,
        new_start: datetime,
        new_end: datetime,
        gap: timedelta,
    ) -> bool:
        for existing_start, existing_end in printer_intervals[pid]:
            if new_start < existing_end + gap and existing_start - gap < new_end:
                return False
        return True

    # ------------------------------------------------------------------
    # 6. Helper: check that this job isn't already assigned to this
    #    printer in the current scheduling attempt.
    # ------------------------------------------------------------------

    def job_not_already_on_printer(
        job_number: str,
        pid: str,
        accumulated: list[dict],
    ) -> bool:
        return not any(
            row["job_number"] == job_number and row["assigned_printer"] == pid
            for row in accumulated
        )

    # ------------------------------------------------------------------
    # 7. Helper: check that the printer has finished its last job by
    #    the time we want to start. With the interval list this is just
    #    asking whether the printer is clear at the candidate window —
    #    already covered by printer_is_clear — but we keep this named
    #    check explicit so it's easy to modify independently.
    #
    #    In practice this merges checks 1 and 2 from the original code.
    # ------------------------------------------------------------------
    # (Handled inside printer_is_clear above — no separate function needed.)

    # ------------------------------------------------------------------
    # 8. Main scheduling loop with retry
    #
    #    Outer while loop retries jobs that failed to find a printer.
    #    fail_count increments only when a full pass places nothing new,
    #    which shifts the candidate start window forward by one gap and
    #    opens up slots that were previously blocked. fail_count resets
    #    to 0 whenever at least one job is successfully placed, so we
    #    only pay the time penalty when we are genuinely stuck.
    #
    #    Safety valve: if a full pass places nothing AND fail_count has
    #    already been incremented past the number of printers, we are
    #    genuinely deadlocked and break to avoid an infinite loop.
    # ------------------------------------------------------------------

    gap = timedelta(minutes=minimum_gap_between_prints_minutes)

    # accumulated holds one result row per print across all passes.
    # We use accumulated_index to update rows in-place on retry rather
    # than appending duplicates.
    accumulated: list[dict] = []
    accumulated_index: dict[tuple, int] = {}  # (job_number, print_index) -> position

    fail_count = 0
    unscheduled = list(job_numbers)  # jobs not yet fully placed

    max_fails = len(eligible_printers) + 1  # deadlock guard

    while unscheduled:
        placed_this_pass = 0
        still_unscheduled = []

        for job_number in unscheduled:
            job_duration = timedelta(minutes=duration_by_job[job_number])

            latest_other_start = latest_start_excluding_job(
                job_number, accumulated, fail_count
            )
            new_start = latest_other_start + gap
            new_end = new_start + job_duration

            printers_this_job = []
            job_fully_placed = True

            for print_index, print_row in enumerate(prints_by_job[job_number]):
                key = (job_number, print_index)

                # # Skip prints that are already successfully scheduled
                # if key in accumulated_index:
                #     existing = accumulated[accumulated_index[key]]
                #     if existing["assigned_printer"] != "":
                #         continue

                assigned_printer = None

                for printer in eligible_printers:
                    pid = printer["equipment_id"]

                    if not printer_is_clear(pid, new_start, new_end, gap):
                        continue

                    if not job_not_already_on_printer(job_number, pid, accumulated):
                        continue

                    assigned_printer = pid
                    break

                result_row = dict(print_row)
                result_row["plan_print_start_datetime"] = new_start.strftime(
                    datetime_fmt
                )
                result_row["estimated_plan_print_end_datetime"] = new_end.strftime(
                    datetime_fmt
                )
                result_row["assigned_printer"] = assigned_printer or ""
                result_row["printer_hood"] = (
                    printer_hood_by_id.get(assigned_printer, "")
                    # if assigned_printer
                    # else ""
                )

                if key in accumulated_index:
                    accumulated[accumulated_index[key]] = result_row
                else:
                    accumulated_index[key] = len(accumulated)
                    accumulated.append(result_row)

                if assigned_printer:
                    printers_this_job.append(assigned_printer)

                    printer_intervals[assigned_printer].append((new_start, new_end))
                    # printer_intervals[assigned_printer].sort(key=lambda x: x[0])
                else:
                    job_fully_placed = False
                    break

            if job_fully_placed:
                placed_this_pass += 1
            else:
                for p in printers_this_job:
                    printer_intervals[p].pop(-1)
                still_unscheduled.append(job_number)

        if placed_this_pass > 0:
            # Progress made — reset fail_count and retry remaining jobs
            fail_count = 0
        else:
            # No progress — push the time window forward and try again
            fail_count += 1
            if fail_count > max_fails:
                # Genuinely stuck, cannot schedule remaining jobs
                break

        unscheduled = still_unscheduled

    # ------------------------------------------------------------------
    # 9. Convert accumulated results back to a Polars DataFrame.
    #    Single construction — no incremental concat.
    # ------------------------------------------------------------------

    if not accumulated:
        return pl.DataFrame()

    confirmed_scheduled_prints = pl.from_dicts(accumulated, schema=schema_dict)

    return confirmed_scheduled_prints
