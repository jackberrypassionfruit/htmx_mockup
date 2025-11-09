With(
    {
        minimum_gap_between_prints_minutes: Value(TextInput16_1.Text),

        eligible_printers:
            Filter(
                Filter(
                    active_printers,
                    condition in ["Production", "Onboarding"],

                    // TESTING - Specifically re-schedule MBraun Prints
                    // StartsWith(printer_hood, "M"),
                    !StartsWith(printer_hood, "M"),


                    // Value(Right(equipment_id, 3)) < 550,
                    !(equipment_id in ComboBox_printers_gone_swimming_1.SelectedItems)
                ),
                1=1
            ),

        initial_production_start_time:
            With(
                {
                    newest_prod_print:
                        First(Sort(
                            Filter(
                                scheduled_prints, 
                                // TESTING - Pull in past prints, Prod and Eng Trial
                                // "Both were originally un-commented
                                // !(scrapped in [ "Cancelled" ]),
                                // request_type = "Production"

                                !(scrapped in [ "Cancelled" ]),
                                // request_type = "Production"

                                
                                // TESTING - Specifically re-schedule MBraun Prints
                                // StartsWith(printer_hood, "M")
                                !StartsWith(printer_hood, "M")

                            ),
                            plan_print_start_datetime,
                            SortOrder.Descending
                        )).plan_print_start_datetime,

                    selected_start_time:
                        DateAdd(
                            DateAdd(DatePicker2_11.SelectedDate, Dropdown5_14.SelectedText.Value, TimeUnit.Hours),
                            Dropdown5_15.SelectedText.Value,
                            TimeUnit.Minutes
                        )
                },
                If(
                    DateDiff(selected_start_time, newest_prod_print, TimeUnit.Minutes) > Value(TextInput16_1.Text), // minimum_gap_between_prints_minutes
                    newest_prod_print,
                    // selected_start_time
                    DateAdd(selected_start_time, -1*Value(TextInput16_1.Text), TimeUnit.Minutes)
                )
            )
    },

    Clear(current_schedule_attempt);
    Clear(confirmed_scheduled_prints);
    Clear(iter);
    Clear(potential_overlapping_prints);
    Clear(pre_clean_unscheduled_prints);
    // Clear(all_current_schedule_attempt);
    Clear(fail_iter);

    ForAll(
        Sequence(100) As i,

        RemoveIf(current_schedule_attempt, true);

        If(
            CountRows(unscheduled_prints) > 0,

            Collect(iter, ["new_iter"]);
            RemoveIf(pre_clean_unscheduled_prints, true);

            ForAll(
                Sort(Distinct(unscheduled_prints, job_number), Value, SortOrder.Ascending) As job,
                // ["6238680"] As job,
                
                With(
                    {
                        latest_scheduled_start_other_job:
                            DateAdd(
                                Coalesce(
                                    First(Sort(Filter(current_schedule_attempt,   job_number <> job.Value), plan_print_start_datetime, SortOrder.Descending)).plan_print_start_datetime,
                                    First(Sort(Filter(confirmed_scheduled_prints, job_number <> job.Value), plan_print_start_datetime, SortOrder.Descending)).plan_print_start_datetime,
                                    initial_production_start_time
                                ),
                                CountRows(fail_iter) * minimum_gap_between_prints_minutes,
                                TimeUnit.Minutes
                            )
                    },
                    With(
                        {
                            new_print_start_time:
                                DateAdd(latest_scheduled_start_other_job, minimum_gap_between_prints_minutes, TimeUnit.Minutes)
                        },
                        With(
                            {
                                new_print_end_time:
                                    DateAdd(
                                        new_print_start_time,
                                        LookUp(unscheduled_prints, job_number=job.Value).estimated_print_time_minutes,
                                        TimeUnit.Minutes
                                    )
                            },
                            RemoveIf(potential_overlapping_prints, true);

                            Collect(
                                potential_overlapping_prints,
                                Filter(
                                    scheduled_prints As x,
                                    !(x.scrapped in [ "Cancelled" ]),
                                    x.plan_print_start_datetime >= DateAdd(
                                        new_print_start_time, 
                                        -1 * (minimum_gap_between_prints_minutes + LookUp(unscheduled_prints, job_number = job.Value).estimated_print_time_minutes), 
                                        TimeUnit.Minutes
                                    ),
                                    x.plan_print_start_datetime <= DateAdd(
                                        new_print_start_time, 
                                        minimum_gap_between_prints_minutes + LookUp(unscheduled_prints, job_number = job.Value).estimated_print_time_minutes, 
                                        TimeUnit.Minutes
                                    )
                                )
                            );

                            ForAll(
                                Filter(
                                    unscheduled_prints, 
                                    job_number = job.Value
                                ) As x,
                                Collect(
                                    current_schedule_attempt,
                                    {
                                        job_number: job.Value,

                                        assigned_printer:
                                            First(
                                                Sort(
                                                    Sort(
                                                        Filter(
                                                            active_printers As printer,
                                                            printer.model_number in x.platform,
                                                            printer.equipment_id in eligible_printers.equipment_id,
                                                            !(printer.equipment_id in Filter(current_schedule_attempt, job_number = x.job_number).assigned_printer),

                                                            
                                                            Coalesce(
                                                                First(Sort(
                                                                    Filter(current_schedule_attempt, assigned_printer = printer.equipment_id), 
                                                                    estimated_plan_print_end_datetime, 
                                                                    SortOrder.Descending)
                                                                ).estimated_plan_print_end_datetime,

                                                                First(Sort(
                                                                    Filter(confirmed_scheduled_prints, assigned_printer = printer.equipment_id), 
                                                                    estimated_plan_print_end_datetime, 
                                                                    SortOrder.Descending)
                                                                ).estimated_plan_print_end_datetime
                                                            ) <= latest_scheduled_start_other_job,
                                                            
                                                            CountRows(
                                                                Filter(
                                                                    potential_overlapping_prints As overlap,
                                                                    new_print_start_time < DateAdd(
                                                                        overlap.estimated_plan_print_end_datetime, 
                                                                        minimum_gap_between_prints_minutes, 
                                                                        TimeUnit.Minutes
                                                                    ) &&
                                                                    DateAdd(
                                                                        overlap.plan_print_start_datetime, 
                                                                        -1 * minimum_gap_between_prints_minutes, 
                                                                        TimeUnit.Minutes
                                                                    ) <= new_print_end_time,

                                                                    overlap.assigned_printer = printer.equipment_id
                                                                )
                                                            ) = 0,

                                                            
                                                            LookUp(
                                                                scheduled_prints As a,
                                                                a.plan_print_start_datetime = new_print_start_time
                                                                // && a.estimated_plan_print_end_datetime = new_print_end_time
                                                                
                                                                && Abs(DateDiff(
                                                                    a.plan_print_start_datetime,
                                                                    new_print_start_time,
                                                                    TimeUnit.Minutes
                                                                )) < minimum_gap_between_prints_minutes
                                                            ).print_number = Blank()



                                                            // TESTING - giving Eng Trial Prints a time bound
                                                            // x.plan_time_begin <= TimeValue(new_print_end_time),
                                                            // TimeValue(new_print_end_time) <= x.plan_time_end,
                                                            // DateDiff(new_print_start_time, new_print_end_time, TimeUnit.Days) = 0
                                                            // END TESTING
                                                            
                                                        ),
                                                        equipment_id
                                                    ),
                                                    printer_hood
                                                )
                                            ).equipment_id,

                                        printer_model: x.printer_model,
                                        plan_print_start_datetime: new_print_start_time,
                                        estimated_plan_print_end_datetime: new_print_end_time,
                                        yellow_print: false,

                                        master_job_map_pk_id: x.master_job_map_pk_id,
                                        order_id: x.order_id,
                                        part_number: x.part_number,
                                        request_type: x.request_type,
                                        die_revision: x.die_revision,
                                        configuration_id: x.configuration_id,
                                        printer_hood: "", // x.printer_hood,
                                        completion_date: x.completion_date,
                                        print_number: x.print_number,
                                        qty_parts: x.qty_parts,
                                        print_file:
                                            x.print_file,
                                        // print_file_name_cfg: // x.print_file_name_cfg,
                                            // x.print_file&"_"&Right(x.assigned_printer, 3),
                                        material_file_name_cfg: x.material_file_name_cfg,
                                        estimated_print_time: x.estimated_print_time,
                                        estimated_print_time_minutes: x.estimated_print_time_minutes,
                                        actual_print_start_datetime: x.actual_print_start_datetime,
                                        actual_print_end_datetime: x.actual_print_end_datetime,
                                        scrapped: x.scrapped
                                        /*
                                        test_next_being: new_print_start_time,
                                        test_next_end: new_print_end_time,
                                        test_attempt: TimeValue(new_print_start_time),
                                        test_begin: x.plan_time_begin,
                                        test_end: x.plan_time_end,
                                        test_exe:   x.plan_time_begin <= TimeValue(new_print_start_time) &&
                                                    TimeValue(new_print_end_time) <= x.plan_time_end &&
                                                    DateDiff(new_print_start_time, new_print_end_time, TimeUnit.Days) = 0
                                        */
                                    }
                                );
                            )
                        )
                    )
                )
            );

            // fix printer_hood to match assigned_printer
            UpdateIf(
                current_schedule_attempt As a,
                true,
                {
                    printer_hood:
                        LookUp(
                            active_printers As x,   
                            x.equipment_id = a.assigned_printer
                        ).printer_hood
                }
            );

            ForAll(
                Distinct(
                    Filter(
                        Distinct(current_schedule_attempt, job_number),
                        !(Value in Distinct(Filter(current_schedule_attempt, assigned_printer = Blank()), job_number))
                    ),
                    Value
                ) As job,
                Collect(
                    confirmed_scheduled_prints,
                    Filter(current_schedule_attempt, job_number = job.Value)
                )
            );
            Collect(
                pre_clean_unscheduled_prints,
                unscheduled_prints
            );
            RemoveIf(
                unscheduled_prints,
                job_number in Distinct(confirmed_scheduled_prints, job_number).Value
            );
            If(
                CountRows(pre_clean_unscheduled_prints) = CountRows(unscheduled_prints),
                Collect(fail_iter, "FAIL")
            );
            /*
            Collect(
                all_current_schedule_attempt,
                AddColumns(
                    ShowColumns(
                        current_schedule_attempt,

                        print_number,
                        job_number,
                        plan_print_start_datetime,
                        printer_hood,
                        assigned_printer,
                        test_next_being,
                        test_next_end,
                        test_attempt,
                        test_begin,
                        test_end,
                        test_exe
                    ),
                    index,
                        i.Value
                )
            );
            */
        )
    )
);

Collect(
    scheduled_prints,
    Filter(
        confirmed_scheduled_prints,
        assigned_printer <> Blank()
    )
);

Select(Icon34_2);

UpdateContext({
    made_changes:
        true
});

If(
    Toggle5_swimlane_admin_mode_1.Value,
    UpdateContext({
        cached_swimlane_prints:
            false
    })
);