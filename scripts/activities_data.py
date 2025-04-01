import firebase_admin
from firebase_admin import credentials, firestore
import os
import datetime
import pytz
import pandas as pd
from collections import defaultdict

# Ajuste de la zona horaria a Bogota
LOCAL_TZ = pytz.timezone("America/Bogota")

def initialize_firebase():
    cred = credentials.Certificate("firebase/serviceAccountKey.json")
    firebase_admin.initialize_app(cred)
    return firestore.client()

def get_hourly_usage_distribution(db, days=7):
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    start_time = now_utc - datetime.timedelta(days=days)
    
    activities_ref = db.collection('activities')
    query = activities_ref.where('startTime', '>=', start_time)
    docs = query.stream()
    

    hour_minutes = defaultdict(float)
    total_minutes = 0.0
    
    for doc in docs:
        data = doc.to_dict()
        start_ts = data.get('startTime')
        end_ts = data.get('endTime')
        
        if start_ts is None or end_ts is None:
            continue

        start_local = start_ts.astimezone(LOCAL_TZ)
        end_local = end_ts.astimezone(LOCAL_TZ)
        
        if end_local <= start_local:
            continue
        
        current_block_start = start_local.replace(minute=0, second=0, microsecond=0)
        
        while current_block_start < end_local:
            end_of_block = current_block_start + datetime.timedelta(hours=1)
            
            block_start = max(current_block_start, start_local)
            block_end = min(end_of_block, end_local)
            
            block_minutes = (block_end - block_start).total_seconds() / 60.0
            if block_minutes > 0:
                hour_minutes[current_block_start.hour] += block_minutes
                total_minutes += block_minutes
            
            current_block_start = end_of_block
    
    df_data = []
    for h in range(24):
        mins = round(hour_minutes[h],3)
        if total_minutes > 0:
            percentage = round((mins / total_minutes),3)
        else:
            percentage = 0
        
        df_data.append({
            "Hour": f"{h:02d}",        
            "Minutes": mins, 
            "Percentage": percentage
        })
    
    df = pd.DataFrame(df_data)
    return df

def save_data_to_csv(df, filename='peak_hours_data.csv'):
    folder_path = "analytics_results"
    os.makedirs(folder_path, exist_ok=True)
    file_path = os.path.join(folder_path, filename)
    df.to_csv(file_path, index=False)
    print(f"Datos guardados en {file_path}")

def main():
    db = initialize_firebase()
    
    df = get_hourly_usage_distribution(db, days=7)
    
    save_data_to_csv(df, 'activities_data.csv')


if __name__ == "__main__":
    main()
