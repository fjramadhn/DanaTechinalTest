import sys
import logging
import staging_engine as frameworkEngine
from datetime import datetime

if __name__ == '__main__':
    #add logging
    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.DEBUG)
    logger = logging.getLogger(__name__)

    #add parameter
    path_file = sys.argv[1]
    table_name = sys.argv[2]
    ingest_type = sys.argv[3]
    logger.info("Job start")
    #call engine
    framework = frameworkEngine.ingestion(table_name, ingest_type)
        
    #get data
    data = framework.readDataFile(path_file)
    #ingestion process Staging
    result = framework.ingestStaging(data)
    logger.info("Job finished. Data {} has been ingested into staging layer: {} rows".format(table_name, result['countdf'][0]))
    #ingestion process ODS
    result = framework.ingestODS()
    logger.info("Job finished. Data {} has been ingested into ODS layer: {} rows".format(table_name, result['countsql'][0]))