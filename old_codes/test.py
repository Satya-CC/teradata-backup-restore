import pandas as pd
import pyarrow.parquet as pq


parquet_path = r"C:\Users\IN45880649\OneDrive - Tesco\Desktop\teradata_export.parquet"

table = pq.read_table(parquet_path)
df_parquet = table.to_pandas()

print(print(df_parquet.dtypes))