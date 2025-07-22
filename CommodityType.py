import pandas as pd
import psycopg2

# Read CSV
df = pd.read_csv(r"C:\Users\VinayPrakashYadav\Downloads\commodity_types_valid.csv")

# PostgreSQL connection
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    dbname="ScriptingDb",  # Make sure this DB exists
    user="postgres",
    password="bitxia"
)
cursor = conn.cursor()
print("✅ Connected to DB.")

# Ensure uuid-ossp extension is enabled
cursor.execute('CREATE EXTENSION IF NOT EXISTS "uuid-ossp";')

# Optional: Clear existing data
# cursor.execute("DELETE FROM commodity_types")
# print("🗑️ Cleared existing data from commodity_types.")

# Insert data
for _, row in df.iterrows():
    cursor.execute("""
        INSERT INTO commodity_types (
            id, commodity_type_name, description, status,
            created_by, updated_by, is_deleted, created_at, updated_at
        )
        VALUES (
            uuid_generate_v4(), %s, %s, %s,
            %s, %s, false, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
        )
    """, (
        row["commodity_type_name"],
        row["description"],
        row["status"],
        row["created_by"],
        row["updated_by"]
    ))

conn.commit()
cursor.close()
conn.close()
print("✅ Inserted data into commodity_types.")
