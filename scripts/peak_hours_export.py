import firebase_admin
from firebase_admin import credentials, firestore
import os
import datetime
import pandas as pd
from collections import defaultdict
import pytz

LOCAL_TZ = pytz.timezone("America/Bogota")

def initialize_firebase():
    cred = credentials.Certificate("firebase/serviceAccountKey.json")
    firebase_admin.initialize_app(cred)
    return firestore.client()

def get_hourly_click_data(db, days=7):
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    start_time = now_utc - datetime.timedelta(days=days)
    
    clicks_ref = db.collection('product-clics')
    query = clicks_ref.where('timestamp', '>=', start_time)
    docs = query.stream()
    
    hour_counts = defaultdict(int)
    total_clicks = 0
    
    for doc in docs:
        data = doc.to_dict()
        ts = data.get('timestamp')
        if ts is not None:
            ts_local = ts.astimezone(LOCAL_TZ)
            hour = ts_local.hour
            hour_counts[hour] += 1
            total_clicks += 1

    df_data = []
    for h in range(24):
        count_h = hour_counts[h]
        percentage_h = round((count_h / total_clicks) if total_clicks > 0 else 0,3)
        
        hour_str = f"{h:02d}"  # "00", "01", "02", ..., "23"
        
        df_data.append({
            'Hour': hour_str,
            'Clicks': count_h,
            'Percentage': percentage_h
        })
    
    df = pd.DataFrame(df_data)
    return df

def save_data_to_csv(df, filename='peak_hours_data.csv'):
    folder_path = "analytics_results"
    os.makedirs(folder_path, exist_ok=True)
    
    file_path = os.path.join(folder_path, filename)
    df.to_csv(file_path, index=False)

def main():
    db = initialize_firebase()

    df = get_hourly_click_data(db, days=7)

    save_data_to_csv(df, 'peak_hours_data.csv')

if __name__ == "__main__":
    main()
