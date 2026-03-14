# ──────────────────────────────────────────────────
# Author: Naveen Vishlavath
# File: delta_lake/load_to_snowflake.py
#
# Loads Delta Lake data into Snowflake.
# Bridge between local Delta Lake and Snowflake
# where dbt models run.
#
# Improvements from v3:
#   - Fixed NAN/NaT issue with proper null handling
#   - Convert all timestamp variants to strings
#   - Replace all null string variants with None
#
# Run after Spark streaming has processed events:
#   python3 delta_lake/load_to_snowflake.py
# ──────────────────────────────────────────────────

import logging
import os
import glob
import sys
from typing import List, Dict, Any

import numpy as np
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

# ── logging ──────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ── snowflake config ─────────────────────────────────
SNOWFLAKE_ACCOUNT   = os.getenv('SNOWFLAKE_ACCOUNT',   'yzjbrlr-ro49347')
SNOWFLAKE_USER      = os.getenv('SNOWFLAKE_USER',      'NAVEENNAYAK657')
SNOWFLAKE_PASSWORD  = os.getenv('SNOWFLAKE_PASSWORD',  '')
SNOWFLAKE_DATABASE  = os.getenv('SNOWFLAKE_DATABASE',  'ECOMMERCE_DB')
SNOWFLAKE_SCHEMA    = os.getenv('SNOWFLAKE_SCHEMA',    'RAW')
SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH')
SNOWFLAKE_ROLE      = os.getenv('SNOWFLAKE_ROLE',      'ACCOUNTADMIN')

# ── delta lake paths ─────────────────────────────────
BRONZE_PATH = os.getenv('BRONZE_PATH', './delta_lake/bronze')
SILVER_PATH = os.getenv('SILVER_PATH', './delta_lake/silver')
GOLD_PATH   = os.getenv('GOLD_PATH',   './delta_lake/gold')

# all null-like string values we want to convert to None
NULL_VALUES = ['nan', 'NaN', 'NAN', 'NaT', 'nat', 'None', 'none', 'NULL', 'null', '<NA>']


def check_dependencies() -> None:
    """Check all required dependencies before running."""
    try:
        import pyarrow
        logger.info(f"✅ pyarrow {pyarrow.__version__} found")
    except ImportError:
        logger.error(
            "❌ pyarrow not installed\n"
            "   Fix: pip3 install pyarrow --break-system-packages"
        )
        sys.exit(1)

    if not SNOWFLAKE_PASSWORD:
        logger.error(
            "❌ SNOWFLAKE_PASSWORD not set in .env file\n"
            "   Fix: add SNOWFLAKE_PASSWORD=yourpassword to .env"
        )
        sys.exit(1)


def get_snowflake_connection():
    """Create and return a Snowflake connection."""
    logger.info(f"🔌 Connecting to Snowflake: {SNOWFLAKE_ACCOUNT}")

    try:
        conn = snowflake.connector.connect(
            account=SNOWFLAKE_ACCOUNT,
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA,
            warehouse=SNOWFLAKE_WAREHOUSE,
            role=SNOWFLAKE_ROLE
        )
        logger.info("✅ Connected to Snowflake successfully")
        return conn

    except Exception as e:
        logger.error(f"❌ Failed to connect to Snowflake: {e}")
        raise


def read_delta_layer(path: str) -> pd.DataFrame:
    """Read all parquet files from a Delta Lake layer."""
    parquet_files = glob.glob(f"{path}/**/*.parquet", recursive=True)

    if not parquet_files:
        logger.warning(f"⚠️  No parquet files found in {path}")
        return pd.DataFrame()

    logger.info(f"📂 Found {len(parquet_files)} parquet files in {path}")

    dfs    = []
    failed = 0

    for file in parquet_files:
        try:
            df = pd.read_parquet(file)
            dfs.append(df)
        except Exception as e:
            logger.warning(f"⚠️  Could not read {file}: {e}")
            failed += 1

    if failed > 0:
        logger.warning(f"⚠️  Failed to read {failed} files")

    if not dfs:
        return pd.DataFrame()

    combined = pd.concat(dfs, ignore_index=True)
    logger.info(f"✅ Read {len(combined)} rows from {path}")
    return combined


def clean_value(val):
    """
    Clean a single value for Snowflake insertion.
    Converts all null-like values to None.
    This is the core fix for the NAN identifier error.
    """
    if val is None:
        return None
    if isinstance(val, float) and np.isnan(val):
        return None
    if isinstance(val, str) and val in NULL_VALUES:
        return None
    if isinstance(val, (dict, list)):
        return str(val)
    # handle pandas NaT
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    return val


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean dataframe before loading to Snowflake.
    - Uppercase column names
    - Convert timestamps to strings
    - Replace all null variants with None
    - Remove duplicates
    """
    if df.empty:
        return df

    original_count = len(df)

    # uppercase column names — Snowflake convention
    df.columns = [col.upper() for col in df.columns]

    # convert ALL datetime/timestamp columns to strings first
    for col in df.columns:
        dtype_str = str(df[col].dtype)
        if 'datetime' in dtype_str or 'timestamp' in dtype_str:
            df[col] = df[col].astype(str)
            # NaT becomes the string 'NaT' — replace with None
            df[col] = df[col].apply(
                lambda x: None if x in NULL_VALUES else x
            )

    # apply clean_value to every cell in the dataframe
    # this catches all remaining null variants
    df = df.applymap(clean_value)

    # remove duplicates safely
    try:
        df = df.drop_duplicates()
        removed = original_count - len(df)
        if removed > 0:
            logger.info(f"🔄 Removed {removed} duplicate rows")
    except Exception as e:
        logger.warning(f"⚠️  Could not deduplicate: {e}")

    return df


def create_table_if_not_exists(
    cursor,
    table_name: str,
    df: pd.DataFrame
) -> None:
    """Create Snowflake table based on DataFrame schema."""
    type_mapping = {
        'object':         'VARCHAR(16777216)',
        'int64':          'NUMBER(38,0)',
        'int32':          'NUMBER(38,0)',
        'float64':        'FLOAT',
        'bool':           'BOOLEAN',
        'datetime64[ns]': 'TIMESTAMP_NTZ',
    }

    columns = []
    for col, dtype in df.dtypes.items():
        sf_type = type_mapping.get(str(dtype), 'VARCHAR(16777216)')
        columns.append(f'"{col}" {sf_type}')

    columns.append(
        '"_LOADED_AT" TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()'
    )

    create_sql = f"""
        CREATE TABLE IF NOT EXISTS
        {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{table_name} (
            {', '.join(columns)}
        )
    """

    cursor.execute(create_sql)
    logger.info(f"✅ Table {table_name} ready in Snowflake")


def validate_row_count(
    cursor,
    table_name: str,
    expected_count: int
) -> bool:
    """Verify row count in Snowflake matches what we loaded."""
    cursor.execute(
        f"SELECT COUNT(*) FROM "
        f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{table_name}"
    )
    actual_count = cursor.fetchone()[0]

    if actual_count != expected_count:
        logger.error(
            f"❌ Row count mismatch for {table_name}!\n"
            f"   Expected : {expected_count}\n"
            f"   Actual   : {actual_count}"
        )
        return False

    logger.info(
        f"✅ Row count validated — {actual_count} rows in {table_name}"
    )
    return True


def load_to_snowflake(
    conn,
    df: pd.DataFrame,
    table_name: str
) -> Dict[str, Any]:
    """Load DataFrame to Snowflake using batch insert."""
    if df.empty:
        logger.warning(f"⚠️  No data to load for {table_name}")
        return {'table': table_name, 'rows': 0, 'status': 'skipped'}

    cursor = conn.cursor()

    try:
        create_table_if_not_exists(cursor, table_name, df)

        cursor.execute(
            f"TRUNCATE TABLE "
            f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{table_name}"
        )

        columns      = ', '.join([f'"{col}"' for col in df.columns])
        placeholders = ', '.join(['%s'] * len(df.columns))
        insert_sql   = f"""
            INSERT INTO
            {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{table_name}
            ({columns}) VALUES ({placeholders})
        """

        # convert each row — apply clean_value to be safe
        rows = []
        for row in df.itertuples(index=False):
            clean_row = tuple(clean_value(val) for val in row)
            rows.append(clean_row)

        batch_size   = 1000
        total_loaded = 0

        for i in range(0, len(rows), batch_size):
            batch         = rows[i:i + batch_size]
            cursor.executemany(insert_sql, batch)
            total_loaded += len(batch)
            logger.info(
                f"  📤 {total_loaded}/{len(rows)} rows "
                f"into {table_name}..."
            )

        conn.commit()

        count_valid = validate_row_count(
            cursor, table_name, total_loaded
        )

        status = 'success' if count_valid else 'warning'
        logger.info(
            f"✅ {table_name} — {total_loaded} rows | status={status}"
        )

        return {
            'table':  table_name,
            'rows':   total_loaded,
            'status': status
        }

    except Exception as e:
        conn.rollback()
        logger.error(f"❌ Failed to load {table_name}: {e}")
        return {'table': table_name, 'rows': 0, 'status': 'failed'}

    finally:
        cursor.close()


def print_load_summary(results: List[Dict]) -> None:
    """Print a clear load summary at the end."""
    logger.info(f"\n{'='*55}")
    logger.info("📋 LOAD SUMMARY")
    logger.info(f"{'='*55}")

    total_rows = 0
    for result in results:
        status_icon = (
            "✅" if result['status'] == 'success'
            else "⚠️ " if result['status'] == 'warning'
            else "❌" if result['status'] == 'failed'
            else "⏭️ "
        )
        logger.info(
            f"  {status_icon} {result['table']:<35} "
            f"{result['rows']:>8} rows"
        )
        total_rows += result['rows']

    logger.info(f"{'─'*55}")
    logger.info(f"  {'TOTAL':<35} {total_rows:>8} rows")
    logger.info(f"{'='*55}\n")


def main():
    logger.info("🚀 Starting Delta Lake → Snowflake Load")
    logger.info(f"   Account   : {SNOWFLAKE_ACCOUNT}")
    logger.info(f"   Database  : {SNOWFLAKE_DATABASE}")
    logger.info(f"   Schema    : {SNOWFLAKE_SCHEMA}")
    logger.info(f"   Warehouse : {SNOWFLAKE_WAREHOUSE}")

    check_dependencies()

    conn    = get_snowflake_connection()
    results = []

    try:
        # ── load bronze events ───────────────────────
        logger.info("\n📥 Loading Bronze → EVENTS...")
        bronze_df = read_delta_layer(BRONZE_PATH)
        if not bronze_df.empty:
            bronze_df = clean_dataframe(bronze_df)
            result    = load_to_snowflake(conn, bronze_df, 'EVENTS')
            results.append(result)

        # ── load gold revenue ────────────────────────
        logger.info("\n📥 Loading Gold → REVENUE_BY_CATEGORY...")
        revenue_df = read_delta_layer(
            f"{GOLD_PATH}/revenue_by_category"
        )
        if not revenue_df.empty:
            revenue_df = clean_dataframe(revenue_df)
            result     = load_to_snowflake(
                conn, revenue_df, 'GOLD_REVENUE_BY_CATEGORY'
            )
            results.append(result)

        # ── load gold payment stats ──────────────────
        logger.info("\n📥 Loading Gold → PAYMENT_STATS...")
        payment_df = read_delta_layer(f"{GOLD_PATH}/payment_stats")
        if not payment_df.empty:
            payment_df = clean_dataframe(payment_df)
            result     = load_to_snowflake(
                conn, payment_df, 'GOLD_PAYMENT_STATS'
            )
            results.append(result)

    finally:
        conn.close()
        logger.info("🔌 Snowflake connection closed")

    print_load_summary(results)

    failed = [r for r in results if r['status'] == 'failed']
    if failed:
        raise Exception(
            f"❌ {len(failed)} table(s) failed — check logs above"
        )

    logger.info("✅ All tables loaded successfully!")


if __name__ == '__main__':
    main()