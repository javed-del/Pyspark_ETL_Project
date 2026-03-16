from src.framework.engine.abstract import ETLAbstract


class S3toS3Service(ETLAbstract):
    def __init__(self, spark):
        self.spark = spark
        self.transactions = None

    def read(self):
        self.transactions = self.spark.read.csv(
            "s3a://pyspark-test-bucket-data/source/ecommerce_orders.csv",
            header=True,
            inferSchema=True
        )

    def transform(self):
        self.transactions = self.transactions.drop("coupon_code", "payment_method")

    def load(self):
        self.transactions.write.mode("overwrite").option("header", "true").parquet(
            "s3a://pyspark-test-bucket-data/destination"
        )

