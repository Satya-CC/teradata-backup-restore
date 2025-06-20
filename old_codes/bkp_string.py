import pandas as pd 
import pyarrow as pa
import pyarrow.parquet as pq
import teradatasql

host = 'TDPG'
username = 'SVC-UCIEXPORT'
password = '$vc@bcs03ej12'
database_name = 'DXWI_PROD_MGA_PLAY_PEN'
table_name = 'DONOT_DELETE_uk_food_supplier_calc_audit_NO24_BACKUP'
sql_query = f"""
SELECT * 
FROM DXWI_PROD_MGA_PLAY_PEN.DONOT_DELETE_uk_food_supplier_calc_audit_NO24_BACKUP
WHERE "Tesco Week" BETWEEN 201801 AND 201853 
"""
output_file = r"C:\Users\IN45880649\OneDrive - Tesco\Desktop\teradata_export.parquet"

# --- Connect ---
conn = teradatasql.connect(
    host=host,
    user=username,
    password=password,
    logmech='LDAP',
)

# Step 1: Get column metadata
query_meta = f"""
SELECT
  ColumnName,
  ColumnType,
  ColumnLength,
  DecimalTotalDigits,
  DecimalFractionalDigits
FROM dbc.columnsV
WHERE databasename = 'DXWI_PROD_MGA_PLAY_PEN'
  AND tablename = 'DONOT_DELETE_uk_food_supplier_calc_audit_NO24_BACKUP'
ORDER BY ColumnId
"""

df_meta = pd.read_sql(query_meta, conn)

# Step 2: Build PyArrow schema
def teradata_to_pyarrow(row):
    t = row['ColumnType'].strip()
    precision = row['DecimalTotalDigits']
    scale = row['DecimalFractionalDigits']

    if t == 'CV':  # VARCHAR
        return pa.field(row['ColumnName'], pa.string())
    elif t == 'I':  # INTEGER
        return pa.field(row['ColumnName'], pa.int32())
    elif t == 'I8':  # BIGINT
        return pa.field(row['ColumnName'], pa.int64())
    elif t == 'D':  # DECIMAL
        return pa.field(row['ColumnName'], pa.decimal128(precision or 18, scale or 0))
    elif t == 'DA':  # DATE
        return pa.field(row['ColumnName'], pa.date32())
    elif t == 'TS':  # TIMESTAMP
        return pa.field(row['ColumnName'], pa.timestamp('s'))
    else:
        return pa.field(row['ColumnName'], pa.string())  # fallback

arrow_fields = [teradata_to_pyarrow(row) for _, row in df_meta.iterrows()]
arrow_schema = pa.schema(arrow_fields)

# Step 3: Load actual data
df_data = pd.read_sql(sql_query, conn)


# Step 4: Convert decimal columns to string first, then fix NaNs
for _, row in df_meta.iterrows():
    col = row['ColumnName']
    if row['ColumnType'].strip() == 'D' and col in df_data.columns:
        df_data[col] = df_data[col].astype(str)
        df_data[col] = df_data[col].replace('nan', None)


# Step 5: Convert to Arrow Table without schema
table = pa.Table.from_pandas(df_data, preserve_index=False)

# Step 6: Apply the correct schema by casting
table = table.cast(arrow_schema)

# Step 7: Write the Arrow table to Parquet
pq.write_table(table, output_file)
print(f"✅ Backup complete: {output_file}")