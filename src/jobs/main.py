from src.services.S3TOS3 import S3toS3Service
from src.framework.engine.spark_context_factory import get_spark_session

spark = get_spark_session()
etl = S3toS3Service(spark)
etl.read()
etl.transform()
etl.load()
