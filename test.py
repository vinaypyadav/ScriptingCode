import pandas as pd
import logging
import sys
from DatabaseConnection import get_postgres_connection
from logger import AppLogger

# Configure logging
logging_handler = logging.StreamHandler(sys.stdout)
logging_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger = AppLogger.get_logger(__name__)
logger.handlers = [logging_handler]

def normalize_param_type(param_type):
    """Standardize parameter types - treat 'Optional (Industrial/Processor)' as 'Optional'"""
    param_type = str(param_type).strip()
    if "industrial" in param_type.lower() or "processor" in param_type.lower():
        return "Optional"
    if param_type in {"Essential", "Optional"}:
        return param_type
    return None

def fetch_id(cursor, table, column, value):
    """Helper function to fetch IDs with case-insensitive matching"""
    try:
        cursor.execute(
            f"SELECT id FROM {table} WHERE LOWER(TRIM({column})) = LOWER(TRIM(%s)) AND is_deleted = false",
            (value,)
        )
        return cursor.fetchone()[0] if cursor.rowcount > 0 else None
    except Exception as e:
        logger.error(f"Error fetching ID from {table}: {str(e)}")
        return None

try:
    # Load Excel file
    df = pd.read_excel("AssayCommodityMaster.xlsx", sheet_name="Sheet1", engine='openpyxl')
    logger.info("Excel file loaded successfully")
    
    # Clean data - replace NaN with empty strings
    df = df.fillna('')
    
    # Connect to DB
    conn = get_postgres_connection()
    cursor = conn.cursor()
    logger.info("Connected to PostgreSQL DB")
    
    # Ensure uuid extension
    cursor.execute('CREATE EXTENSION IF NOT EXISTS "uuid-ossp";')
    conn.commit()

    valid_records = 0
    skipped_records = 0

    for index, row in df.iterrows():
        try:
            # Extract and clean values
            commodity = str(row.get("Commodity", "")).strip()
            raw_param_type = str(row.get("Parameter Type", "")).strip()
            param_type = normalize_param_type(raw_param_type)
            parameter_name = str(row.get("Parameter Name", "")).strip()
            uom = str(row.get("UoM", "")).strip()
            measurement_method = str(row.get("Measurement Unit / Method", "")).strip()
            sample_size = str(row.get("Sample size", "")).strip()

            # Validate required fields
            if not all([commodity, param_type, parameter_name, uom, measurement_method, sample_size]):
                logger.debug(f"Skipping row {index} - missing required fields")
                skipped_records += 1
                continue

            # Get foreign keys
            commodity_id = fetch_id(cursor, "commodities", "commodity_name", commodity)
            parameter_id = fetch_id(cursor, "assaying_component_master", "assaying_parameter", parameter_name)
            measurement_type_id = fetch_id(cursor, "measurement_component_master", "measurement_type", measurement_method)
            sampling_unit_id = fetch_id(cursor, "uom", "uom_name", uom)

            # Skip if any reference is missing
            if None in [commodity_id, parameter_id, measurement_type_id, sampling_unit_id]:
                logger.debug(f"Skipping row {index} - missing reference data")
                skipped_records += 1
                continue

            # Handle sequence number
            try:
                sequence_no = int(float(row.get("Sequence No", index + 1)))
            except (ValueError, TypeError):
                sequence_no = index + 1

            # Prepare other fields
            range_1 = str(row.get("Range-1 (Min - Max)", "")).strip() or None
            range_2 = str(row.get("Range-2 (Min - Max)", "")).strip() or None
            range_3 = str(row.get("Range-3 (Min - Max)", "")).strip() or None
            faq_range = str(row.get("FAQ Range", "")).strip() or None

            # Insert valid record
            cursor.execute("""
                INSERT INTO commodity_wise_assaying_details (
                    id, commodity_id, parameter_type, sequence_no, assaying_parameter_id,
                    range_1, range_2, range_3, sample_size, sampling_unit_id,
                    measurement_type_id, faq_range, is_active, is_deleted,
                    created_by, updated_by, created_at, updated_at
                ) VALUES (
                    uuid_generate_v4(), %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, true, false,
                    uuid_generate_v4(), uuid_generate_v4(), CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                )
            """, (
                commodity_id, param_type, sequence_no, parameter_id,
                range_1, range_2, range_3, sample_size, sampling_unit_id,
                measurement_type_id, faq_range
            ))
            
            valid_records += 1
            
        except Exception as e:
            logger.error(f"Error processing row {index}: {str(e)}")
            skipped_records += 1
            continue

    conn.commit()
    logger.info(f"Data insertion completed. Success: {valid_records}, Skipped: {skipped_records}")

except Exception as e:
    logger.error(f"Script failed: {str(e)}")
    if conn:
        conn.rollback()
    sys.exit(1)

finally:
    if cursor:
        cursor.close()
    if conn:
        conn.close()
        logger.info("Database connection closed")