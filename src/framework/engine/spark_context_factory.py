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
        # Local / Docker / CI environment
        from pyspark.sql import SparkSession

        aws_key = os.environ.get("AWS_ACCESS_KEY_ID", "")
        aws_secret = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
        aws_region = os.environ.get("AWS_REGION", "us-east-1")

        builder = SparkSession.builder.appName("MySparkApplication")

        if aws_key and aws_secret:
            builder = builder \
                .config("spark.hadoop.fs.s3a.access.key", aws_key) \
                .config("spark.hadoop.fs.s3a.secret.key", aws_secret) \
                .config("spark.hadoop.fs.s3a.endpoint", f"s3.{aws_region}.amazonaws.com") \
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

        return builder.getOrCreate()