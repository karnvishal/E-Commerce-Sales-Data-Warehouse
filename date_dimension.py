import pandas as pd
from datetime import datetime, timedelta

def generate_date_dimension(start_date, end_date):
    date_range = pd.date_range(start=start_date, end=end_date)
    
    df = pd.DataFrame({
        'date_id': date_range.strftime('%Y%m%d'),
        'date_day': date_range,
        'date_week': date_range.to_period('W').start_time,
        'date_month': date_range.to_period('M').start_time,
        'date_quarter': date_range.to_period('Q').start_time,
        'date_year': date_range.to_period('Y').start_time,
        'day_of_week': date_range.dayofweek + 1, 
        'day_of_month': date_range.day,
        'week_of_year': date_range.isocalendar().week,
        'month_of_year': date_range.month,
        'quarter_of_year': date_range.quarter,
        'year': date_range.year,
        'day_name': date_range.strftime('%A'),
        'month_name': date_range.strftime('%B'),
        'is_weekend': (date_range.dayofweek >= 5).astype(int),
        'is_holiday': 0  
    })
    
    # Convert datetime to date
    for col in ['date_day', 'date_week', 'date_month', 'date_quarter', 'date_year']:
        df[col] = df[col].dt.date
    
    return df

date_df = generate_date_dimension('2025-01-01', '2025-05-31')
date_df.to_csv('C:/Users/lenovo/OneDrive/Documents/Ecom e2e/ecommerce_dbt/ecommerce_dbt/seeds/date_dimension.csv', index=False)