from .extract import schema_dict
import polars as pl
from dateutil import parser
from datetime import datetime, timedelta

datetime_fmt = "%Y-%m-%d %H:%M:%S"


def str_to_dt(strd):
    return datetime.strptime(strd, datetime_fmt)


def partition_prints_by_printer_ordered_w_style(today_prints_clean, selected_date):
    prints_by_printer = [
        (
            str.strip(task.select(pl.first("assigned_printer")).item())[-3:],
            ("0" + str.strip(task.select(pl.first("printer_hood")).item()))[-2:],
            str.strip(task.select(pl.first("printer_model")).item()),
            [
                print
                | {
                    "tindex": tindex,
                    "print_width_style": f"calc({print['estimated_print_time_minutes'] / 17.5}% - 20px)",
                    "x_coord_style": str(
                        (
                            0.85
                            * (
                                str_to_dt(print["plan_print_start_datetime"])
                                - selected_date
                            ).total_seconds()
                            / 60
                            # / 2.9
                            # - (tindex * print["estimated_print_time_minutes"])
                        )
                        + 160
                    )
                    + "px",
                    "debug": print["job_number"],
                    # print["plan_print_start_datetime"][-8:],
                    # (
                    #     str_to_dt(print["plan_print_start_datetime"])
                    #     - selected_date
                    # ).total_seconds(),
                }
                for tindex, print in enumerate(task.to_dicts())
            ],
        )
        for task in today_prints_clean.sort(
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
        # pl.col("platform").str.starts_with("Core 530X")
        | (
            pl.col("plan_print_start_datetime").str.to_datetime(datetime_fmt)
            < pl.lit(selected_datetime)
        ),
    )

    pending_tasks = today_prints.filter(
        # ~pl.col("request_type").str.starts_with("Production"),
        ~pl.col("printer_hood").str.starts_with("M"),
        # ~pl.col("platform").str.starts_with("Core 530X"),
        pl.col("plan_print_start_datetime").str.to_datetime(datetime_fmt)
        >= pl.lit(selected_datetime),
    )

    return (today_prints_filtered, pending_tasks)


def schedule_cached_prints_old(
    min_gap_minutes,
    schedule_start_time,
    available_resources,
    committed_tasks,
    pending_tasks,
):
    checkpoint_now = datetime.now()
    checkpoint_then = checkpoint_now
    checkpoint_start = checkpoint_now
    i = 1
    print(f"Checkpoint #{i=}")
    print(checkpoint_now - checkpoint_then, "\n\n")

    resources = sorted(
        available_resources.filter(
            ~pl.col("printer_hood").str.starts_with("M")
        ).to_dicts(),
        key=lambda x: (x["printer_hood"], x["equipment_id"]),
        reverse=False,
    )

    newest_scheduled_task_time = (
        committed_tasks.filter(~pl.col("printer_hood").str.starts_with("M"))
        .select(pl.max("plan_print_start_datetime"))
        .item()
    )

    if not newest_scheduled_task_time:
        initial_start_time = schedule_start_time - timedelta(minutes=min_gap_minutes)
    elif (
        str_to_dt(newest_scheduled_task_time) - schedule_start_time
    ).total_seconds() / 60 > min_gap_minutes:
        initial_start_time = str_to_dt(newest_scheduled_task_time)
    else:
        initial_start_time = schedule_start_time - timedelta(minutes=min_gap_minutes)

    iter_count = 0
    gap_offset_index = 0
    new_scheduled_tasks = pl.DataFrame()

    test_l = []

    for i in range(1):
        current_schedule_attempt = pl.DataFrame()
        if len(pending_tasks) > 0:
            iter_count += 1

            for job_number in (
                pl.Series(
                    pending_tasks.unique(subset="job_number").select("job_number")
                )
                .sort()
                .to_list()
            ):
                this_job_estimated_print_time_minutes = (
                    pending_tasks.filter(pl.col("job_number") == pl.lit(job_number))
                    .select(pl.first("estimated_print_time_minutes"))
                    .item()
                )

                latest_scheduled_start_other_job = (
                    (
                        None
                        if current_schedule_attempt.is_empty()
                        else (
                            current_schedule_attempt.filter(
                                pl.col("job_number") != pl.lit(job_number)
                            )
                            .select(pl.max("plan_print_start_datetime"))
                            .item()
                        )
                    )
                    or (
                        None
                        if new_scheduled_tasks.is_empty()
                        else str_to_dt(
                            new_scheduled_tasks.filter(
                                pl.col("job_number") != pl.lit(job_number)
                            )
                            .select(pl.max("plan_print_start_datetime"))
                            .item()
                        )
                    )
                    or initial_start_time
                ) + timedelta(minutes=gap_offset_index * min_gap_minutes)

                new_print_start_time = latest_scheduled_start_other_job + timedelta(
                    minutes=min_gap_minutes
                )

                new_print_end_time = new_print_start_time + timedelta(
                    minutes=this_job_estimated_print_time_minutes
                )

                potential_overlapping_prints = committed_tasks.filter(
                    pl.col("plan_print_start_datetime").str.to_datetime(datetime_fmt)
                    >= (
                        new_print_start_time
                        - timedelta(
                            minutes=min_gap_minutes
                            + this_job_estimated_print_time_minutes
                        )
                    ),
                    pl.col("plan_print_start_datetime").str.to_datetime(datetime_fmt)
                    <= (
                        new_print_start_time
                        + timedelta(
                            minutes=min_gap_minutes
                            + this_job_estimated_print_time_minutes
                        )
                    ),
                )

                for print_this_job in pending_tasks.filter(
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
                    for printer in resources:
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
                        last_end_time_new_scheduled_tasks = (
                            None
                            if new_scheduled_tasks.is_empty()
                            else (
                                new_scheduled_tasks.filter(
                                    pl.col("assigned_printer")
                                    == pl.lit(printer["equipment_id"])
                                )
                            )
                            .select(pl.max("estimated_plan_print_end_datetime"))
                            .item()
                        )

                        is_this_printer_finished_with_last_print = (
                            last_end_time_current_schedule_attempt is None
                            and last_end_time_new_scheduled_tasks is None
                        ) or (
                            str_to_dt(
                                last_end_time_current_schedule_attempt
                                or last_end_time_new_scheduled_tasks
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
                                + pl.duration(minutes=min_gap_minutes),
                                pl.col("plan_print_start_datetime").str.to_datetime(
                                    datetime_fmt
                                )
                                - pl.duration(minutes=min_gap_minutes)
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
                available_resources.select("equipment_id", "printer_hood"),
                left_on=["assigned_printer"],
                right_on=["equipment_id"],
                how="inner",
            )

            new_scheduled_tasks = pl.concat(
                [new_scheduled_tasks, current_schedule_attempt]
            )

    # print(
    #     new_scheduled_tasks.select(
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

    # for p in resources:
    #     print(f'{p['printer_hood']=}, {p['equipment_id']=}')

    return new_scheduled_tasks


def schedule_cached_prints(
    min_gap_minutes,
    schedule_start_time,
    available_resources,
    committed_tasks,
    pending_tasks,
    num_worker=2,
):
    # Filter to non-M printers and sort, same as before
    resources = sorted(
        available_resources.filter(
            ~pl.col("printer_hood").str.starts_with("M")
        ).to_dicts(),
        key=lambda x: (x["printer_hood"], x["equipment_id"]),
    )
    resources_ids = [p["equipment_id"] for p in resources]

    # Build a lookup from equipment_id -> printer_hood for the fix-up step at the end
    resource_hood_by_id = {p["equipment_id"]: p["printer_hood"] for p in resources}

    # Determine initial production start time (same logic as before)
    newest_scheduled_task_time = (
        # committed_tasks.filter(~pl.col("printer_hood").str.starts_with("M"))
        committed_tasks.filter(~pl.col("platform").str.starts_with("Core 530X"))
        .select(pl.max("plan_print_start_datetime"))
        .item()
    )

    if not newest_scheduled_task_time:
        initial_start_time = schedule_start_time - timedelta(minutes=min_gap_minutes)
    elif (
        str_to_dt(newest_scheduled_task_time) - schedule_start_time
    ).total_seconds() / 60 > min_gap_minutes:
        initial_start_time = str_to_dt(newest_scheduled_task_time)
    else:
        initial_start_time = schedule_start_time - timedelta(minutes=min_gap_minutes)

    # Convert committed_tasks to dicts once
    committed_tasks_dicts = committed_tasks.filter(
        ~pl.col("printer_hood").str.starts_with("M")
    ).to_dicts()

    # task_intervals: dict[printer_id -> list of (start_dt, end_dt)]
    task_intervals: dict[str, list[tuple[datetime, datetime]]] = {
        resource_id: [] for resource_id in resources_ids
    }
    for task in committed_tasks_dicts:
        resource_id = task["assigned_printer"]
        if resource_id not in task_intervals.keys():
            continue
        start = str_to_dt(task["plan_print_start_datetime"])
        end = str_to_dt(task["estimated_plan_print_end_datetime"])
        task_intervals[resource_id].append((start, end))

    for resource_id in task_intervals:
        task_intervals[resource_id].sort(key=lambda x: x[0])

    cached_dicts = pending_tasks.to_dicts()

    # Unique sorted job numbers
    job_ids = sorted(
        pending_tasks.unique(subset="job_number")
        .select("job_number")
        .to_series()
        .to_list()
    )

    # Group cached tasks by job_number
    tasks_by_job: dict[str, list[dict]] = {job_id: [] for job_id in job_ids}
    for task in cached_dicts:
        tasks_by_job[task["job_number"]].append(task)

    # Duration per job (same for all tasks in a job)
    duration_by_job: dict[str, float] = {
        job_id: tasks_by_job[job_id][0]["estimated_print_time_minutes"]
        for job_id in job_ids
    }

    def get_new_print_start_time(
        job_id: str,
        scheduled_tasks: list[dict],
        gap_offset_index: int,
        gap,
        num_worker: int,
    ) -> datetime:
        start_times_other_jobs = [
            task["plan_print_start_datetime"]
            for task in scheduled_tasks
            if task["job_number"] != job_id
        ]
        if start_times_other_jobs:
            last_start_str = max(start_times_other_jobs)
            jobs_at_last_print_time = set(
                [
                    task["job_number"]
                    for task in scheduled_tasks
                    if task["plan_print_start_datetime"] == last_start_str
                    and task["job_number"] != job_id
                    and task["assigned_printer"] != ""
                ]
            )
            last_start = str_to_dt(last_start_str)
            if len(jobs_at_last_print_time) >= num_worker:
                last_start += gap
        else:
            last_start = initial_start_time + gap
        return last_start + timedelta(minutes=gap_offset_index * min_gap_minutes)

    gap = timedelta(minutes=min_gap_minutes)

    # scheduled_tasks holds one result task per print across all passes.
    # We use scheduled_task_index_map to update tasks in-place on retry rather
    # than appending duplicates.
    scheduled_tasks: list[dict] = []
    scheduled_task_index_map: dict[tuple, int] = {}  # (job_number, tindex) -> position

    gap_offset_index = 0
    unscheduled_tasks = list(job_ids)  # jobs not yet fully placed

    max_fails = len(resources) + 1  # deadlock guard

    while unscheduled_tasks:
        scheduled_tasks_this_pass = False
        still_unscheduled_tasks = []

        for job_id in unscheduled_tasks:
            job_duration = timedelta(minutes=duration_by_job[job_id])

            new_start = get_new_print_start_time(
                job_id, scheduled_tasks, gap_offset_index, gap, num_worker
            )
            new_end = new_start + job_duration

            resources_this_job = []
            job_fully_placed = True

            for tindex, each_task in enumerate(tasks_by_job[job_id]):
                key = (job_id, tindex)

                assigned_resource = None

                for this_resource in resources:
                    resource_id = this_resource["equipment_id"]

                    # ------------------------------------------------------------------
                    #    Check whether a candidate (start, end) window overlaps
                    #    any existing interval on a given printer, respecting the gap.
                    #
                    #    Two intervals [A_start, A_end] and [B_start, B_end] conflict if:
                    #       A_start < B_end + gap  AND  B_start - gap < A_end
                    #
                    #    task_intervals[resource_id] is already sorted, so we could binary
                    #    search, but linear scan is fine for typical schedule sizes.
                    # ------------------------------------------------------------------
                    noverlap = True
                    for existing_start, existing_end in task_intervals[resource_id]:
                        if (
                            new_start < existing_end + gap
                            and existing_start - gap < new_end
                        ):
                            noverlap = False
                    if not noverlap:
                        continue

                    # ------------------------------------------------------------------
                    #    Check that this job isn't already assigned to this
                    #    printer in the current scheduling attempt.
                    # ------------------------------------------------------------------
                    if any(
                        task["job_number"] == job_id
                        and task["assigned_printer"] == resource_id
                        for task in scheduled_tasks
                    ):
                        continue

                    assigned_resource = resource_id
                    break

                result_task = dict(each_task)
                result_task["plan_print_start_datetime"] = new_start.strftime(
                    datetime_fmt
                )
                result_task["estimated_plan_print_end_datetime"] = new_end.strftime(
                    datetime_fmt
                )
                result_task["assigned_printer"] = assigned_resource or ""
                result_task["printer_hood"] = resource_hood_by_id.get(
                    assigned_resource, ""
                )

                if key in scheduled_task_index_map:
                    scheduled_tasks[scheduled_task_index_map[key]] = result_task
                else:
                    scheduled_task_index_map[key] = len(scheduled_tasks)
                    scheduled_tasks.append(result_task)

                if assigned_resource:
                    gap_offset_index = 0
                    resources_this_job.append(assigned_resource)

                    task_intervals[assigned_resource].append((new_start, new_end))
                    # task_intervals[assigned_printer].sort(key=lambda x: x[0])
                else:
                    job_fully_placed = False
                    break

            if job_fully_placed:
                scheduled_tasks_this_pass = True
            else:
                for p in resources_this_job:
                    task_intervals[p].pop()
                still_unscheduled_tasks.append(job_id)

        if not scheduled_tasks_this_pass:
            # No progress — push the time window forward and try again
            gap_offset_index += 1
            if gap_offset_index > max_fails:
                # Genuinely stuck, cannot schedule remaining jobs
                break

        unscheduled_tasks = still_unscheduled_tasks

    if not scheduled_tasks:
        return pl.DataFrame()

    new_scheduled_tasks = pl.from_dicts(scheduled_tasks, schema=schema_dict)

    return new_scheduled_tasks
