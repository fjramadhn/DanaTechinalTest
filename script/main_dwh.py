import os
import sys
import logging
import pandas as pd
from sqlalchemy import create_engine
from os.path import dirname, abspath
from datetime import datetime
from helper import *

class dwh():
    def __init__(self, tablename, ingest_type='init'):
        self.ingest_type = ingest_type
        self.tablename = tablename
        self.schema = pd.read_json(f'{dirname(dirname(abspath(__file__)))}/metadata/{self.tablename}.json')
        self.user = os.environ.get("USER")
        self.baseQuery = open(f'{dirname(dirname(abspath(__file__)))}/query/{self.tablename}.sql').read().format(self.user)
        self.cnx = create_engine("postgresql://ubuntu:ubuntu@localhost/airflow").connect()
        self.delta = pd.read_sql(f"select delta_col, last_snapshot from dwh.delta_snapshot where tablename = '{self.tablename}'", con=self.cnx)
        self.col = self.delta['delta_col'][0]
        self.delta_snapshot = self.delta['last_snapshot'][0]

    def eksekusi(self):
        column_list = self.schema['column_name'].tolist()
        columns = ', '.join(column_list)
        column_update = construct_update(column_list)
        if self.ingest_type == 'init':
            self.cnx.execute(f"DROP TABLE IF EXISTS dwh.{self.tablename};")
            query = f"CREATE TABLE dwh.{self.tablename} as {self.baseQuery};"
        else:
            query = f"""INSERT INTO dwh.{self.tablename} ({columns}, ingesttime, ingestby) 
                        {self.baseQuery} WHERE {self.col} > {self.delta_snapshot}
                        ON CONFLICT (date) DO UPDATE SET {column_update}, ingesttime=exclude.ingesttime, ingestby=exclude.ingestby;"""
        self.cnx.execute(query)
        result = self.updateLog()

        return result

    def updateLog(self):
        last_snapshot = pd.read_sql(f"SELECT MAX({self.col}) as delta from dwh.{self.tablename}", con=self.cnx)
        self.cnx.execute(f"""UPDATE dwh.delta_snapshot
                            SET last_snapshot = '{last_snapshot['delta'][0]}', ingesttime = now()
                            WHERE tablename = '{self.tablename}'""")
        query = f"SELECT COUNT(1) as countresult FROM dwh.{self.tablename}"
        if self.ingest_type == 'delta':
            query = query + f" WHERE {self.col} > '{self.delta_snapshot}' and ingestby = '{self.user}'"
        resultdf = pd.read_sql(query, con=self.cnx)
        resultdf["tablename"], resultdf["ingesttime"] = [self.tablename, datetime.now()]
        resultdf.to_sql("result_process", schema='dwh', con=self.cnx, if_exists="append", index=False)

        return resultdf

if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.DEBUG)
    logger = logging.getLogger(__name__)
    #get parameter
    table_name = sys.argv[1]
    ingest_type = sys.argv[2]
    logger.info("Job start")
    #start process
    frameworkEngine = dwh(table_name, ingest_type)
    result = frameworkEngine.eksekusi()
    logger.info("Job finished. Data {} has been ingested into DWH layer: {} rows".format(table_name, result['countresult'][0]))