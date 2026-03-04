import json
import random
from datetime import datetime, timedelta
import os

# Create data/raw/transactions directory
os.makedirs("data/raw/transactions", exist_ok=True)

# Generate Sample Users (Dimension Table)
def generate_users(num_users=1000):
    users = []
    for i in range(num_users):
        user_id = f"user_{i}"
        users.append({
            "user_id": user_id,
            "name": f"User {i}",
            "email": f"user_{i}@example.com",
            "region": random.choice(["North", "South", "East", "West", "Central"]),
            "created_at": (datetime.now() - timedelta(days=random.randint(1, 365))).isoformat()
        })
    return users

# Generate Sample Merchants (Dimension Table)
def generate_merchants(num_merchants=500):
    merchants = []
    for i in range(num_merchants):
        merchant_id = f"merchant_{i}"
        merchants.append({
            "merchant_id": merchant_id,
            "merchant_name": f"Merchant {i}",
            "category": random.choice(["E-commerce", "Retail", "Services", "Travel"]),
            "location": f"City {random.randint(1, 100)}"
        })
    return merchants

# Generate Sample Transactions (Fact Table)
def generate_transactions(num_transactions=100000, num_users=1000, num_merchants=500):
    f = None
    for i in range(num_transactions):
        # Create a skewed distribution for users (e.g., 20% of users do 80% of transactions)
        if random.random() < 0.8:
            user_id = f"user_{random.randint(0, int(0.2 * num_users))}"
        else:
            user_id = f"user_{random.randint(0, num_users - 1)}"

        merchant_id = f"merchant_{random.randint(0, num_merchants - 1)}"
        transaction_id = f"txn_{i}"
        amount = round(random.uniform(5.0, 500.0), 2)
        currency = random.choice(["USD", "EUR", "GBP", "JPY", "INR"])
        status = random.choice(["Completed", "Pending", "Failed"])
        timestamp = (datetime.now() - timedelta(minutes=random.randint(1, 1440))).isoformat()

        transaction = {
            "transaction_id": transaction_id,
            "user_id": user_id,
            "merchant_id": merchant_id,
            "amount": amount,
            "currency": currency,
            "status": status,
            "transaction_timestamp": timestamp
        }
        
        # Save in chunks of 5000 transactions to simulate HDFS files
        if i % 5000 == 0:
            if f: f.close()
            filename = f"data/raw/transactions/part_{i//5000}.json"
            f = open(filename, "w")
        
        f.write(json.dumps(transaction) + "\n")
        
    if f: f.close()

if __name__ == "__main__":
    print("Generating users...")
    users = generate_users()
    with open("data/dim_users.json", "w") as f:
        for u in users:
            f.write(json.dumps(u) + "\n")

    print("Generating merchants...")
    merchants = generate_merchants()
    with open("data/dim_merchants.json", "w") as f:
        for m in merchants:
            f.write(json.dumps(m) + "\n")

    print("Generating 100k transactions (with skew)...")
    generate_transactions(num_transactions=100000)
    print("Data generation complete.")
