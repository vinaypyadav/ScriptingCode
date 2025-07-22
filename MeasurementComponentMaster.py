import pandas as pd
from DatabaseConnection import get_postgres_connection
from logger import AppLogger

logger = AppLogger.get_logger(__name__)

# ✅ Load the Excel file into a DataFrame
assayingComponentMaster = pd.read_excel(
    "C:/Users/VinayPrakashYadav/Desktop/ScriptingCode/AssayCommodityMaster.xlsx"
)

conn = get_postgres_connection()
cursor = conn.cursor()
logger.info("Connected to PostgreSQL DB.")

# Ensure uuid-ossp extension is enabled
cursor.execute('CREATE EXTENSION IF NOT EXISTS "uuid-ossp";')

# ✅ Track already inserted (trimmed and lowercased) parameter names to avoid duplicates
seen_parameters = set()

for _, row in assayingComponentMaster.iterrows():
    measurement_type = str(row.get("Measurement Unit / Method", "")).strip()

    if not measurement_type:
        continue  # Skip empty names

    normalized_name = measurement_type.lower()

    if normalized_name in seen_parameters:
        continue  # Skip duplicates in Excel

    # ✅ Check if parameter already exists in DB (case-insensitive)
    cursor.execute("""
        SELECT 1 FROM measurement_component_master
        WHERE LOWER(TRIM(measurement_type)) = %s AND is_deleted = false
    """, (normalized_name,))
    
    exists = cursor.fetchone()
    if exists:
        continue  # Skip if already present in DB

    seen_parameters.add(normalized_name)

    cursor.execute("""
        INSERT INTO measurement_component_master (
            id, measurement_type,is_active,
            created_by, updated_by, is_deleted, created_at, updated_at
        )
        VALUES (
            uuid_generate_v4(), %s,true,
            uuid_generate_v4(), uuid_generate_v4(), false, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
        )
    """, (
        measurement_type,
    ))

conn.commit()
logger.info("Data insertion completed.")
