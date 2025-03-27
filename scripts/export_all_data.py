import pandas as pd
from scripts.utils import get_firestore_client
from datetime import datetime

def export_all_products():
    db = get_firestore_client()
    products_ref = db.collection("products")
    docs = products_ref.stream()

    products = []
    for doc in docs:
        data = doc.to_dict()
        data["id"] = doc.id
        products.append(data)

    df = pd.DataFrame(products)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = f"data/raw/products_{timestamp}.csv"
    df.to_csv(output_path, index=False)
    print(f"✅ Products exported as: {output_path}")

if __name__ == "__main__":
    export_all_products()