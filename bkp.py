print('4')
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from decimal import Decimal
import numpy as np
from td_connection import get_connection


database_name = 'DXWI_PROD_MGA_PLAY_PEN'
table_name = 'DONOT_DELETE_uk_food_supplier_calc_audit_NO24_BACKUP'
start_week = '202001'
end_week = '202054'
sql_query = f"""
SELECT * 
FROM {database_name}.{table_name}
WHERE "Tesco Week" BETWEEN {start_week} AND {end_week} 
"""
output_file = r"C:\Users\IN45880649\OneDrive - Tesco\Desktop\DONOT_DELETE_uk_food_supplier_calc_audit_NO24_BACKUP_202101_202113.parquet"

# --- Connect ---
conn = get_connection()

# Step 1: Fetch metadata
query_meta = f"""
SELECT
  ColumnName,
  ColumnType,
  ColumnLength,
  DecimalTotalDigits,
  DecimalFractionalDigits
FROM dbc.columnsV
WHERE databasename = '{database_name}'
  AND tablename = '{table_name}'
ORDER BY ColumnId
"""
df_meta = pd.read_sql(query_meta, conn)

# Step 2: Build PyArrow schema from Teradata types
def teradata_to_pyarrow(row):
    t = row['ColumnType'].strip()
    precision = row['DecimalTotalDigits']
    scale = row['DecimalFractionalDigits']
    name = row['ColumnName']

    if t == 'CV': return pa.field(name, pa.string())
    elif t == 'I': return pa.field(name, pa.int32())
    elif t == 'I8': return pa.field(name, pa.int64())
    elif t == 'D':
        return pa.field(name, pa.decimal128(precision or 18, scale or 0))
    elif t == 'DA': return pa.field(name, pa.date32())
    elif t == 'TS': return pa.field(name, pa.timestamp('s'))
    else: return pa.field(name, pa.string())

arrow_fields = [teradata_to_pyarrow(row) for _, row in df_meta.iterrows()]
arrow_schema = pa.schema(arrow_fields)

# Step 3: Read data
df_data = pd.read_sql(sql_query, conn)

# Step 4: Cast column types before converting to Arrow
for _, row in df_meta.iterrows():
    col = row['ColumnName']
    t = row['ColumnType'].strip()

    if col not in df_data.columns:
        continue

    try:
        if t == 'I':
            df_data[col] = pd.to_numeric(df_data[col], errors='coerce').astype('Int32')
        elif t == 'I8':
            df_data[col] = pd.to_numeric(df_data[col], errors='coerce').astype('Int64')
        elif t == 'D':
            # Cast to Decimal instead of float
            df_data[col] = df_data[col].apply(lambda x: Decimal(str(x)) if pd.notnull(x) else None)
        elif t == 'DA':
            df_data[col] = pd.to_datetime(df_data[col], errors='coerce').dt.date
        elif t == 'TS':
            df_data[col] = pd.to_datetime(df_data[col], errors='coerce')
    except Exception as e:
        print(f"⚠️ Could not convert column '{col}': {e}")

# Step 5: Convert to Arrow Table
table = pa.Table.from_pandas(df_data, schema=arrow_schema, preserve_index=False)

# Step 6: Write to Parquet
pq.write_table(table, output_file)

print(f"✅ Backup complete: {output_file}")
