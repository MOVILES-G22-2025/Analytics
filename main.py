import firebase_admin
from firebase_admin import credentials, firestore
import os
import datetime
import pandas as pd

# Inicializar Firebase
def initialize_firebase():
    cred = credentials.Certificate("serviceAccountKey.json") 
    firebase_admin.initialize_app(cred)
    return firestore.client()

# Obtener cantidad de usuarios registrados en la última semana
def get_weekly_new_users(db):
    one_week_ago = datetime.datetime.now(datetime.UTC) - datetime.timedelta(days=7)
    
    users_ref = db.collection('users')
    query = users_ref.where('createdAt', '>=', one_week_ago)
    
    results = query.stream()
    return len(list(results))

# Obtener cantidad de inicios de sesión en la última semana
def get_weekly_logins(db):
    one_week_ago = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=7)
    
    logins_ref = db.collection('logins')
    query = logins_ref.where('timestamp', '>=', one_week_ago)
    
    results = query.stream()
    return len(list(results))

# Guardar datos en CSV
def save_data_to_csv(new_users, logins):
    data = {
        "Metric": ["New Users", "Logins"],
        "Count": [new_users, logins],
    }
    
    df = pd.DataFrame(data)

    # Crear carpeta si no existe
    folder_path = "analytics_results"
    os.makedirs(folder_path, exist_ok=True)

    file_path = os.path.join(folder_path, "weekly_activity.csv")
    df.to_csv(file_path, index=False)

# Función principal
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