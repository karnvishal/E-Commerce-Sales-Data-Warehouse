import os
from datetime import datetime, timedelta
from google.cloud import storage
from pathlib import Path

def upload_to_gcs(bucket_name, source_folder, target_prefix, date):
    """Uploads daily data files to GCS with date partitioning
    
    Args:
        bucket_name: GCS bucket name (e.g., 'ecommerce-dw-raw')
        source_folder: Local folder containing date-partitioned data
        target_prefix: GCS prefix (e.g., 'orders', 'order_items')
        date: Date object for the data being uploaded
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    
    date_str = date.strftime('%Y-%m-%d')
    local_path = Path(source_folder) / date_str
    print(f"Looking for files in: {local_path}")
    if not local_path.exists():
            raise FileNotFoundError(f"No directory found at {local_path}")
            
    for filename in os.listdir(local_path):
        if filename.endswith('.csv'):
            local_file = local_path / filename
            if not local_file.exists():
                raise FileNotFoundError(f"File not found: {local_file}")
    
    if not local_path.exists():
        print(f"No data found for {date_str}")
        return
    
    for filename in os.listdir(local_path):
        if not filename.endswith('.csv'):
            continue
            
        local_file = local_path / filename
        blob_path = f"{target_prefix}/{date_str}/{filename}"
        blob = bucket.blob(blob_path)
        
        blob.upload_from_filename(str(local_file))
        print(f"Uploaded {local_file} to gs://{bucket_name}/{blob_path}")

    

def upload_daily_data(date):
    """Orchestrates upload of all daily files to GCS"""
    
    bucket_name = 'ecommerce-dw-raw'
    base_data_dir = Path('/opt/airflow/data') 
    

    upload_to_gcs(
        bucket_name,
        base_data_dir / 'orders',
        'orders',
        date
    )
    upload_to_gcs(
        bucket_name,
        base_data_dir / 'order_items',
        'order_items',
        date
    )
    upload_to_gcs(
        bucket_name,
        base_data_dir / 'inventory',
        'inventory',
        date
    )
    
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    
    for ref_file in ['customers.csv', 'products.csv']:
        ref_path = base_data_dir / ref_file
        blob = bucket.blob(f'reference/{ref_file}')
        if not blob.exists() and ref_path.exists():
            blob.upload_from_filename(str(base_data_dir / ref_file))
            print(f"Uploaded reference data: {ref_file}")
        elif not ref_path.exists():
            print(f"Reference file not found locally: {ref_file}")
        else:
            print(f"Reference file already exists in GCS: {ref_file}")



if __name__ == "__main__":
    # Default to yesterday's date when run manually
    target_date = datetime.now().date() - timedelta(days=1)
    upload_daily_data(target_date)