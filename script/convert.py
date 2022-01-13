import os
import pandas as pd

path = sys.argv[1]
filename, filetype = os.path.splitext(path)
chunked = pd.read_json(path, lines=True, chunksize=10000)
for idx, df in enumerate(chunked):
    df.to_csv(f"{filename}_idx.csv", index=False, sep='|')
    
    
