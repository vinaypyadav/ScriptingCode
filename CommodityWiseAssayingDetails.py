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
    if pd.isna(param_type):
        return None
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
        result = cursor.fetchone()
        return result[0] if result else None
    except Exception as e:
        logger.error(f"Error fetching ID from {table}: {str(e)}")
        return None

def validate_row(row):
    """Validate all required fields in a row"""
    required_fields = {
        "Commodity": str(row.get("Commodity", "")).strip(),
        "Parameter Type": normalize_param_type(row.get("Parameter Type", "")),
        "Parameter Name": str(row.get("Parameter Name", "")).strip(),
        "UoM": str(row.get("UoM", "")).strip(),
        "Measurement Unit / Method": str(row.get("Measurement Unit / Method", "")).strip(),
        "Sample size": str(row.get("Sample size", "")).strip()
    }
    
    missing_fields = [field for field, value in required_fields.items() if not value]
    if missing_fields:
        return False, f"Missing required fields: {', '.join(missing_fields)}"
    
    return True, ""

try:
    # Load Excel file with formula evaluation
    df = pd.read_excel("AssayCommodityMaster.xlsx", sheet_name="Sheet1", engine='openpyxl')
    logger.info("Excel file loaded successfully with formula evaluation")
    
    # Connect to DB
    conn = get_postgres_connection()
    cursor = conn.cursor()
    logger.info("Connected to PostgreSQL DB")
    
    # Ensure uuid extension
    cursor.execute('CREATE EXTENSION IF NOT EXISTS "uuid-ossp";')
    conn.commit()

    valid_records = 0
    skipped_records = 0
    skip_reasons = {}

    for index, row in df.iterrows():
        try:
            # Validate row first
            is_valid, validation_msg = validate_row(row)
            if not is_valid:
                logger.warning(f"SKIPPED ROW {index}: {validation_msg}")
                skip_reasons[validation_msg] = skip_reasons.get(validation_msg, 0) + 1
                skipped_records += 1
                continue

            # Extract validated values
            commodity = str(row.get("Commodity", "")).strip()
            param_type = normalize_param_type(row.get("Parameter Type", ""))
            parameter_name = str(row.get("Parameter Name","")).strip()
            uom = str(row.get("UoM", "")).strip()
            measurement_method = str(row.get("Measurement Unit / Method", "")).strip()
            sample_size = str(row.get("Sample size", "")).strip()

            # Get sequence number (handle formula results)
            try:
                sequence_no = int(row.get("Sequence No", index + 1))
            except (ValueError, TypeError):
                sequence_no = index + 1
                logger.warning(f"Using fallback sequence number {sequence_no} for row {index}")

            # Get foreign keys
            commodity_id = fetch_id(cursor, "commodities", "commodity_name", commodity)
            parameter_id = fetch_id(cursor, "assaying_component_master", "assaying_parameter", parameter_name)
            measurement_type_id = fetch_id(cursor, "measurement_component_master", "measurement_type", measurement_method)
            sampling_unit_id = fetch_id(cursor, "uom", "uom_name", uom)

            # Check references
            missing_refs = []
            if not commodity_id: missing_refs.append(f"Commodity '{commodity}'")
            if not parameter_id: missing_refs.append(f"Parameter '{parameter_name}'")
            if not measurement_type_id: missing_refs.append(f"Measurement method '{measurement_method}'")
            if not sampling_unit_id: missing_refs.append(f"UOM '{uom}'")

            if missing_refs:
                reason = f"Missing reference data: {', '.join(missing_refs)}"
                logger.warning(f"SKIPPED ROW {index}: {reason}")
                skip_reasons[reason] = skip_reasons.get(reason, 0) + 1
                skipped_records += 1
                continue

            # Prepare other fields
            # Prepare range fields - convert empty strings to NULL for PostgreSQL
            def clean_range(value):
                """Convert range values, handling empty strings and NaN properly"""
                if pd.isna(value) or str(value).strip() == '':
                    return None  # Will become NULL in PostgreSQL
                return str(value).strip()
            
            range_1 = clean_range(row.get("Range-1 (Min - Max)"))
            range_2 = clean_range(row.get("Range-2 (Min - Max)"))
            range_3 = clean_range(row.get("Range-3 (Min - Max)"))
            faq_range = clean_range(row.get("FAQ Range"))or None

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
            reason = f"Unexpected error: {str(e)}"
            logger.error(f"SKIPPED ROW {index}: {reason}")
            skip_reasons[reason] = skip_reasons.get(reason, 0) + 1
            skipped_records += 1
            continue

    conn.commit()
    
    # Print summary
    logger.info("\n=== SKIPPED RECORDS SUMMARY ===")
    for reason, count in skip_reasons.items():
        logger.info(f"{count} records skipped because: {reason}")
    
    logger.info(f"\nData insertion completed. Success: {valid_records}, Skipped: {skipped_records}")

except Exception as e:
    logger.error(f"Script failed: {str(e)}", exc_info=True)
    if conn:
        conn.rollback()
    sys.exit(1)

finally:
    if cursor:
        cursor.close()
    if conn:
        conn.close()
        logger.info("Database connection closed")