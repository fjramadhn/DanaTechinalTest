import os
import re
import sys
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from os.path import dirname, abspath
from datetime import datetime
from helper import *

class ingestion():
    def __init__(self, tablename, ingest_type='init'):
        self.tablename = tablename
        self.schema = pd.read_json(f'{dirname(dirname(abspath(__file__)))}/metadata/{self.tablename}.json')
        self.ingest_type = ingest_type
        self.user = os.environ.get("USER")
        self.cnx = create_engine("postgresql://ubuntu:ubuntu@localhost/airflow").connect()#postgresql://postgres:secret123@localhost").connect()

    
    def readDataFile(self, path):
        filename, filetype = os.path.splitext(path)
        print(filename, filetype)
        if filetype == '.json':
            chunked = pd.read_json(path, lines=True, chunksize=10000)
        else:
            chunked = pd.read_csv(path, iterator=True, chunksize=10000)

        return chunked
    
    def ingestStaging(self, data):
        if self.ingest_type == "init":
            query = queryDDL(self.schema, self.tablename, 'staging')
            self.cnx.execute(f"DROP TABLE IF EXISTS staging.{self.tablename};")
            self.cnx.execute(query)
        else:
            self.cnx.execute(f"TRUNCATE staging.{self.tablename};")
        a = 0
        for idx, df in enumerate(data):
            df = self.verifData(df)
            ingesttime = datetime.now()
            df["ingesttime"] = ingesttime
            df["ingestby"] = self.user #in case using ldap
            df.to_sql(f"{self.tablename}", schema='staging', con=self.cnx, if_exists='append', index=False)
            a += df.shape[0]
        resultdf = pd.read_sql(f"SELECT COUNT(1) as countsql FROM staging.{self.tablename}", con=self.cnx)
        resultdf["countdf"], resultdf["tablename"], resultdf["ingesttime"] = [a, self.tablename, datetime.now()]
        resultdf.to_sql("result_process", schema='staging', con=self.cnx, if_exists="append", index=False)

        return resultdf

    def verifData(self, df):
        df.columns = self.schema["column_name"].tolist()
        df = df.applymap(lambda x: str(int(x)) if (hasattr(x, 'is_integer') and x.is_integer()) else x)
        for i in self.schema["column_type"].unique():
            for col in self.schema.loc[self.schema["column_type"]==i]["column_name"]:
                if i.lower() in ["varchar", "text", 'string']:
                    df[col] = df[col].astype(str)
                elif i.lower() in ["timestamp", "datetime"]:
                    df[col] = pd.datetime(df[col])
                elif i.lower() in ["date"]:
                    df[col] = df[col].apply(lambda x: datetime.strptime(str(x), '%Y%m%d') if type(x) == int or str else x)
                elif i.lower() in ["int", "float"]:
                    df[col] = df[col].apply(lambda x: np.nan if re.search('[a-zA-Z]', str(x)) else x)
                    if i.lower() == 'float':
                        df[col] = df[col].astype(float)
        return df
    
    def ingestODS(self):
        if self.ingest_type == "init":
            query = queryDDL(self.schema, self.tablename, 'ods')
            self.cnx.execute(f"DROP TABLE IF EXISTS ods.{self.tablename};")
            self.cnx.execute(query)
        column_list = self.schema['column_name'].tolist()
        columns = ', '.join(column_list)
        column_update = construct_update(column_list)
        ingesttime = datetime.now()
        query = f"""INSERT INTO ods.{self.tablename} ({columns}, ingesttime, ingestby)
                    SELECT {columns}, '{ingesttime}' as ingesttime, '{self.user}' as ingestby FROM staging.{self.tablename}
                    ON CONFLICT (date) DO UPDATE SET {column_update}, ingesttime=exclude.ingesttime, ingestby=exclude.ingestby;"""
        self.cnx.execute(query)
        resultdf = pd.read_sql(f"SELECT COUNT(1) as countsql FROM ods.{self.tablename} where ingesttime = '{ingesttime}' and ingestby = '{self.user}'", con=self.cnx)
        resultdf["tablename"], resultdf["ingesttime"] = [self.tablename, datetime.now()]
        resultdf.to_sql("result_process", schema='ods', con=self.cnx, if_exists="append", index=False)
        return resultdf