{% macro generate_date_spine(datepart, start_date, end_date) %}
    {# 
        Generates a date spine for analysis.
        Parameters:
            datepart: day/week/month/quarter/year
            start_date: 'YYYY-MM-DD' or date expression
            end_date: 'YYYY-MM-DD' or date expression
    #}
    
    {% set sql %}
        WITH date_spine AS (
            SELECT
                date_day,
                DATE_TRUNC(date_day, WEEK) as date_week,
                DATE_TRUNC(date_day, MONTH) as date_month,
                DATE_TRUNC(date_day, QUARTER) as date_quarter,
                DATE_TRUNC(date_day, YEAR) as date_year,
                EXTRACT(DAYOFWEEK FROM date_day) as day_of_week,
                EXTRACT(DAY FROM date_day) as day_of_month,
                EXTRACT(WEEK FROM date_day) as week_of_year,
                EXTRACT(MONTH FROM date_day) as month_of_year,
                EXTRACT(QUARTER FROM date_day) as quarter_of_year,
                EXTRACT(YEAR FROM date_day) as year,
                FORMAT_DATE('%A', date_day) as day_name,
                FORMAT_DATE('%B', date_day) as month_name,
                DATE_DIFF(date_day, DATE('{{ start_date }}'), DAY) as days_since_start
            FROM UNNEST(
                GENERATE_DATE_ARRAY(
                    DATE('{{ start_date }}'), 
                    DATE('{{ end_date }}'), 
                    INTERVAL 1 DAY
                )
            ) AS date_day
        )
        SELECT
            date_day,
            date_week,
            date_month,
            date_quarter,
            date_year,
            day_of_week,
            day_of_month,
            week_of_year,
            month_of_year,
            quarter_of_year,
            year,
            day_name,
            month_name,
            days_since_start
        FROM date_spine
    {% endset %}
    
    {{ return(sql) }}
{% endmacro %}