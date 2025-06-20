import pandas as pd
import pyarrow.parquet as pq
import teradatasql
import numpy as np
from decimal import Decimal

# --- Connection ---
conn = teradatasql.connect(
    host='TDPG',
    user='SVC-UCIEXPORT',
    password='$vc@bcs03ej12',
    logmech='LDAP'
)

# --- Config ---
input_file = r"C:\Users\IN45880649\OneDrive - Tesco\Desktop\teradata_export.parquet"
target_table = 'DXWI_PROD_MGA_PLAY_PEN.DONOT_DELETE_uk_food_supplier_calc_audit_NO24_BACKUP_RESTORED'

# --- Step 1: Read Parquet ---
table = pq.read_table(input_file)
df = table.to_pandas()

# --- Step 2: Clean NaNs ---
df.replace({np.nan: None}, inplace=True)

# --- Step 3: Convert timestamp columns to string in correct format ---
for col in df.columns:
    if pd.api.types.is_datetime64_any_dtype(df[col]):
        df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')  # string format
        print(f"⏱ Converted timestamp column '{col}' to string")

# --- Step 4: Prepare INSERT SQL ---
columns = df.columns.tolist()
placeholders = ', '.join(['?'] * len(columns))
column_list = ', '.join([f'"{col}"' for col in columns])
insert_sql = f'INSERT INTO {target_table} ({column_list}) VALUES ({placeholders})'

# --- Step 5: Load to Teradata ---
print(f"🚀 Loading {len(df)} rows to {target_table}...")
cursor = conn.cursor()
try:
    cursor.executemany(insert_sql, df.values.tolist())
    print("✅ Restore completed successfully.")
except Exception as e:
    print("❌ Restore failed:", e)
finally:
    cursor.close()
    conn.close()