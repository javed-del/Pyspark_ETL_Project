import os


def get_spark_session():
    """
    Returns SparkSession for local, or GlueContext spark session for AWS Glue.
    Detects environment via IS_GLUE env variable or presence of awsglue module.
    """
    is_glue = os.environ.get("IS_GLUE", "false").lower() == "true"

    if is_glue:
        # AWS Glue environment
        from awsglue.context import GlueContext
        from awsglue.job import Job
        from awsglue.utils import getResolvedOptions
        from pyspark.context import SparkContext
        import sys

        args = getResolvedOptions(sys.argv, ['JOB_NAME'])
        sc = SparkContext()
        glue_context = GlueContext(sc)
        spark = glue_context.spark_session
        return spark

    else:
        # Local environment
        from pyspark.sql import SparkSession

        spark = SparkSession.builder \
            .appName("MySparkApplication") \
            .getOrCreate()
        return spark