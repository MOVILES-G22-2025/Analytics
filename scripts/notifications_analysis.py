import firebase_admin
from firebase_admin import credentials, firestore
import pandas as pd
import os
from datetime import datetime, timedelta

# Inicializar Firebase
def initialize_firebase():
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # Sube de scripts/ a raíz del proyecto
    SERVICE_ACCOUNT_PATH = os.path.join(BASE_DIR, "firebase", "serviceAccountKey.json")

    cred = credentials.Certificate(SERVICE_ACCOUNT_PATH)    
    firebase_admin.initialize_app(cred)
    return firestore.client()

def analyze_push_notifications(db):
    users_ref = db.collection("users")
    interactions_ref = db.collection("notification_interactions")

    # Usuarios que habilitaron notificaciones
    enabled_users_query = users_ref.where("notificationsEnabled", "==", True).stream()
    enabled_user_ids = [doc.id for doc in enabled_users_query]

    # Total de usuarios
    total_users_query = users_ref.stream()
    total_users = sum(1 for _ in total_users_query)

    # Usuarios que interactuaron con notificaciones
    one_week_ago = datetime.utcnow() - timedelta(days=7)
    interactions_query = interactions_ref.where("timestamp", ">=", one_week_ago).stream()

    interacted_user_ids = set()
    interaction_types = {"opened": 0, "ignored": 0, "dismissed": 0}
    total_interactions = 0

    for doc in interactions_query:
        data = doc.to_dict()
        interacted_user_ids.add(data.get("userId"))
        interaction_type = data.get("interactionType")
        if interaction_type in interaction_types:
            interaction_types[interaction_type] += 1
        total_interactions += 1

    enabled_and_interacted = [uid for uid in enabled_user_ids if uid in interacted_user_ids]

    # Resultados
    total_enabled = len(enabled_user_ids)
    total_interacted = len(enabled_and_interacted)
    percentage_interacted = ((total_interacted / total_enabled) * 100) if total_enabled > 0 else 0

    percentage_enabled_users = ((total_enabled / total_users) * 100) if total_users > 0 else 0
    percentage_disabled_users = (100 - percentage_enabled_users) if total_users > 0 else 0

    percentage_interaction_types = {
        key: ((value / total_interactions) * 100) if total_interactions > 0 else 0
        for key, value in interaction_types.items()
    }

    # Crear DataFrame
    df = pd.DataFrame([{
        "Date": datetime.now().strftime("%Y-%m-%d"),
        "Users with Notifications Enabled": total_enabled,
        "Users Interacted": total_interacted,
        "Percentage Interacted (%)": percentage_interacted,
        "Percentage Enabled Users (%)": percentage_enabled_users,
        "Percentage Disabled Users (%)": percentage_disabled_users,
        "Percentage Opened Interactions (%)": percentage_interaction_types["opened"],
        "Percentage Ignored Interactions (%)": percentage_interaction_types["ignored"],
        "Percentage Dismissed Interactions (%)": percentage_interaction_types["dismissed"]
    }])

    # Guardar en CSV
    folder_path = "analytics_results"
    os.makedirs(folder_path, exist_ok=True)
    file_path = os.path.join(folder_path, "push_notifications_stats.csv")
    if os.path.exists(file_path):
        existing_df = pd.read_csv(file_path)
        df = pd.concat([existing_df, df], ignore_index=True)
    df.to_csv(file_path, index=False)
    print(f"Datos guardados en {file_path}")

def main():
    db = initialize_firebase()
    analyze_push_notifications(db)

if __name__ == "__main__":
    main()
