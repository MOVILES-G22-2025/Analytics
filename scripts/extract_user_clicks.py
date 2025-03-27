import firebase_admin
from firebase_admin import credentials, firestore
import pandas as pd
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Firebase configuration
SERVICE_ACCOUNT_PATH = os.getenv("SERVICE_ACCOUNT_PATH")
FIREBASE_PROJECT_ID = os.getenv("FIREBASE_PROJECT_ID")
FIREBASE_DATABASE_URL = os.getenv("FIREBASE_DATABASE_URL")

# Initialize Firebase Admin
if not firebase_admin._apps:
    cred = credentials.Certificate(SERVICE_ACCOUNT_PATH)
    firebase_admin.initialize_app(cred, {
        'projectId': FIREBASE_PROJECT_ID,
        'databaseURL': FIREBASE_DATABASE_URL
    })

# Connect to Firestore
db = firestore.client()

# Query all users
users_ref = db.collection("users")
users = users_ref.stream()

# Store data
data = []

for user in users:
    user_data = user.to_dict()
    user_id = user.id
    category_clicks = user_data.get("categoryClicks", {})

    for category, clicks in category_clicks.items():
        data.append({
            "userId": user_id,
            "category": category,
            "clicks": clicks
        })

# Convert to DataFrame
df = pd.DataFrame(data)

# Sort data
df = df.sort_values(by=["userId", "clicks"], ascending=[True, False])

# Output path
output_path = "data/raw/user_clicks.csv"
os.makedirs(os.path.dirname(output_path), exist_ok=True)

# Save to CSV
df.to_csv(output_path, index=False)

print(f"✅ Data exported to {output_path}")