import os

from pyspark.errors.exceptions.base import AnalysisException
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    coalesce,
    col,
    datediff,
    lit,
    try_to_date,
    when,
)
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from .bronzeLayer import BaseLayer


class SilverLayer(
    BaseLayer,
):
    def __init__(self, session: SparkSession, BronzeDF: DataFrame, ref_data_path: str):
        super().__init__()
        self.logger.info(
            f"Initiating SilverLayer reference data read from: {ref_data_path}"
        )

        self.session = session

        try:
            self.reference = self.session.read.option("samplingRatio", 0.2).csv(
                ref_data_path,
                header=True,
                inferSchema=True,
            )
        except AnalysisException as e:
            self.logger.error(
                f"Reference data missing at {ref_data_path}. Proceeding with empty reference data. : {e}"
            )
            # Deterministic fallback: create empty reference DF to prevent join failures
            empty_schema = StructType(
                [
                    StructField("user_id", StringType(), True),
                    StructField("country", StringType(), True),
                    StructField("signup_date", StringType(), True),
                ]
            )
            self.reference = self.session.createDataFrame([], empty_schema)
        except Exception as e:
            self.logger.error(f"Unexpected error reading reference data: {e}")
            raise
        self.df = BronzeDF

        return None

    def BasicEnrichment(self):
        self.logger.info("Starting Basic Enrichment (Joining reference data).")

        if self.df.isEmpty():
            self.logger.warning("BronzeDF is empty. Bypassing Basic Enrichment.")
            return self

        self.reference = self.reference.withColumn(
            "signup_date", try_to_date(col("signup_date"), "yyyy-MM-dd")
        )

        self.df = self.df.join(self.reference, on="user_id", how="left")

        self.logger.info("SUCCESS: Basic enriched data.")

        return self

    def AdvancedEnrichment(self):

        try:
            if self.df.isEmpty():
                self.logger.warning(
                    "Input DataFrame is empty. Bypassing Advanced Enrichment."
                )
                return self

            self.df = (
                self.df.withColumn("country", coalesce(col("country"), lit("UNKNOWN")))
                .withColumn(
                    "is_purchase",
                    when(col("event_type") == "PURCHASE", True).otherwise(False),
                )
                .withColumn(
                    "days_since_signup", datediff(col("event_date"), col("signup_date"))
                )
                .drop("event_ts_parsed")
            )

            self.df = self.df.filter(col("event_date").isNotNull())

            self.logger.info("SUCCESS: Advanced enriched data.")

        except Exception as e:
            self.logger.error(f"Error during Advanced Enrichment: {e}. Bypassing step.")
        return self

    def write_data(self, output_path: str):

        self.logger.info(f"Writing Silver layer data to {output_path}")
        try:
            if self.df.isEmpty():
                self.logger.warning("Silver DataFrame is empty. Bypassing write.")
                return self

            os.makedirs(output_path, exist_ok=True)

            self.df.write.mode("overwrite").partitionBy("event_date").parquet(
                output_path
            )

            self.logger.info("SUCCESS: Silver layer data written idempotently.")

        except Exception as e:
            self.logger.error(f"Failed to write Silver data: {e}")

        return self
