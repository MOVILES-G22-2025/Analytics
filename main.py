import firebase_admin
from firebase_admin import credentials, firestore
import os
import datetime
import pandas as pd

#Initialize Firebase
def initialize_firebase():
    cred = credentials.Certificate("firebase/serviceAccountKey.json") 
    firebase_admin.initialize_app(cred)
    return firestore.client()

"""BQ TYPE 1: How many users create an account and log in per week?"""
#Get the number of registered users in the last week
def get_weekly_new_users(db):
    one_week_ago = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=7)
    
    users_ref = db.collection('users')
    query = users_ref.where('createdAt', '>=', one_week_ago)
    
    results = query.stream()
    return len(list(results))

#Get the number of logins in the last week
def get_weekly_logins(db):
    one_week_ago = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=7)
    
    logins_ref = db.collection('logins')
    query = logins_ref.where('timestamp', '>=', one_week_ago)
    
    results = query.stream()
    return len(list(results))

#Save data to CSV (Appending data instead of overwriting)
def save_data_to_csv(new_users, logins):
    folder_path = "analytics_results"
    os.makedirs(folder_path, exist_ok=True)

    file_path = os.path.join(folder_path, "weekly_activity.csv")

    #Create DataFrame with the new data
    new_data = pd.DataFrame({
        "Date": [datetime.datetime.now().strftime("%Y-%m-%d")],
        "New Users": [new_users],
        "Logins": [logins]
    })

    #If the file exists, load the previous data
    if os.path.exists(file_path):
        existing_data = pd.read_csv(file_path)
        updated_data = pd.concat([existing_data, new_data], ignore_index=True)
    else:
        updated_data = new_data

    #Save the file without overwriting previous data
    updated_data.to_csv(file_path, index=False)

#Main function
def main():
    print("Connecting to Firebase...")
    db = initialize_firebase()
    
    print("Fetching weekly analytics data...")
    new_users = get_weekly_new_users(db)
    logins = get_weekly_logins(db)
    
    print("Weekly analytics data saved successfully")
    save_data_to_csv(new_users, logins)

if __name__ == "__main__":
    main()
