
import pandas as pd
import pandavro as pdx
import sys

rs = pdx.from_avro(sys.argv[1])
df = pd.DataFrame.from_records(rs)
df.to_parquet(sys.argv[2])