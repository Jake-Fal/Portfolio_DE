-- Date dimension table
-- Provides calendar dates for time-based analysis

{{ dbt_utils.date_spine(
    datepart="day",
    start_date="cast('" ~ var('date_spine_start') ~ "' as date)",
    end_date="cast(current_date + interval '1 year' as date)"
) }}
