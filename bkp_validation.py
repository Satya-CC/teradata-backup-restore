import pandas as pd
import numpy as np
import pyarrow.parquet as pq
import teradatasql
from td_connection import get_connection


sql_query = """
SELECT * 
FROM DXWI_PROD_MGA_PLAY_PEN.DONOT_DELETE_uk_food_supplier_calc_audit_NO24_BACKUP
WHERE "Tesco Week" = 202501
"""
parquet_path = r"C:\Users\IN45880649\OneDrive - Tesco\Desktop\teradata_export.parquet"

# --- CONNECT ---
print("🔄 Reading source data from Teradata...")
conn = get_connection()
df_source = pd.read_sql(sql_query, conn)

print("📦 Reading data from Parquet...")
table = pq.read_table(parquet_path)
df_parquet = table.to_pandas()

# --- VALIDATION ---
print("\n--- ✅ ROW COUNT CHECK ---")
print("Rows in source: ", len(df_source))
print("Rows in Parquet:", len(df_parquet))
if len(df_source) == len(df_parquet):
    print("✅ Row count matches.")
else:
    print("❌ Row count mismatch.")

print("\n--- ✅ COLUMN NAME CHECK ---")
if list(df_source.columns) == list(df_parquet.columns):
    print("✅ Column names match.")
else:
    print("❌ Column name mismatch.")
    print("Source cols :", df_source.columns.tolist())
    print("Parquet cols:", df_parquet.columns.tolist())

print("\n--- ✅ DATA VALUE CHECK ---")
mismatch_count = 0
for col in df_source.columns:
    src = df_source[col]
    tgt = df_parquet[col]

    # Attempt to coerce both sides to numeric if one is numeric and the other is object
    if pd.api.types.is_numeric_dtype(src) and pd.api.types.is_object_dtype(tgt):
        try:
            tgt = pd.to_numeric(tgt, errors='coerce')
        except:
            pass
    elif pd.api.types.is_object_dtype(src) and pd.api.types.is_numeric_dtype(tgt):
        try:
            src = pd.to_numeric(src, errors='coerce')
        except:
            pass

    # Print dtype info
    print(f"🔍 Comparing '{col}' | Source dtype: {src.dtype}, Parquet dtype: {tgt.dtype}")

    if pd.api.types.is_numeric_dtype(src) and pd.api.types.is_numeric_dtype(tgt):
        if not np.allclose(src.fillna(0), tgt.fillna(0), equal_nan=True):
            print(f"❌ Mismatch in column: {col}")
            mismatch_count += 1
    else:
        if not src.fillna("").astype(str).equals(tgt.fillna("").astype(str)):
            print(f"❌ Mismatch in column: {col}")
            mismatch_count += 1

if mismatch_count == 0:
    print("\n✅✅✅ All data values match exactly!")
else:
    print(f"\n❌ Total mismatched columns: {mismatch_count}")
