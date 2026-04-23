import logging
import os
from posix import listdir

from pyspark.errors.exceptions.base import AnalysisException
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, try_to_date, upper
from pyspark.sql.types import StructType

from .schema import AntiDuplicate


class BaseLayer:
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.INFO)
        if not self.logger.handlers:
            ch = logging.StreamHandler()
            ch.setFormatter(
                logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
            )
            self.logger.addHandler(ch)


class BronzeLayer(BaseLayer):
    def __init__(
        self, session: SparkSession, path: str, schema: StructType = None
    ) -> None:
        super().__init__()
        self.path = path
        self.session = session
        self.schema = schema
        self.df = session.createDataFrame([], StructType([]))

        pass

    def readEvent(self) -> "BronzeLayer":
        self.logger.info(f"Starting ingestion from: {self.path}")

        skltn = self.session.read.option("mode", "PERMISSIVE").option(
            "columnNameOfCorruptRecord", "_corrupt_record"
        )

        try:
            if self.schema == None:
                self.logger.warning(
                    "Noted :No explicit schema provided; falling back to schema inference."
                )
                # will be using default mode of spark given because we need the the bad record to be saved to analyze later
                # the aggresive way is use the failfast and informed the engineer ASAP
                # if every data has value for the company
                self.df = skltn.json(self.path)

            else:
                self.logger.info("SUCCESS : Applying Explicit Schema for ingestion")
                self.df = skltn.json(
                    self.path,
                    mode="PERMISSIVE",
                    schema=self.schema,
                    columnNameOfCorruptRecord="_corrupt_record",
                )

            if self.df.isEmpty():
                self.logger.warning(
                    f"Data source is empty at {self.path}. Proceeding with empty DataFrame."
                )
                print("handle empty")

            else:
                self.df = self.df

        except AnalysisException as e:
            self.logger.error(
                f"ERROR : Path unresolved or missing: {self.path}. Gracefully returning empty DataFrame. Details: {e}"
            )
        except Exception as e:
            self.logger.error(
                f"ERROR : Unexpected read failure: {e}. Gracefully returning empty DataFrame."
            )

        return self

    def handleData(self, QuarentinePath: str) -> "BronzeLayer":

        if self.df is None or self.df.isEmpty():
            self.logger.warning("Handling empty DataFrame. Bypassing transformations.")
            print("handle empty")
            return self

        self.df = self.df.withColumn("event_date", try_to_date("event_ts"))
        self.df.cache()

        quarentine = self.df.filter(col("_corrupt_record").isNotNull())
        total_records = self.df.count()
        quarentine_count = quarentine.count()

        if total_records > 0 and quarentine_count == total_records:
            self.logger.critical(
                f"100% schema mismatch detected. All {total_records} records corrupted."
            )
        elif quarentine_count > 0:
            self.logger.warning(
                f"Quarantined {quarentine_count} invalid records out of {total_records}."
            )
        else:
            self.logger.info(f"All {total_records} records passed schema validation.")

        if quarentine_count > 0:
            os.makedirs(QuarentinePath, exist_ok=True)
            self.logger.info(
                "Writing quarantine data using dynamic partition overwrite by 'event_date'."
            )
            quarentine.write.mode("overwrite").partitionBy("event_date").parquet(
                QuarentinePath
            )

        self.df = (
            (self.df.filter(col("_corrupt_record").isNull()).drop("_corrupt_record"))
            .withColumn(
                "rn", row_number().over(AntiDuplicate("event_id", "event_ts", "value"))
            )
            .filter(col("rn") == 1)
            .drop("rn")
            .withColumn("event_type", upper(col("event_type")))
            .fillna({"value": 0})
        )

        self.logger.info(
            "SUCCESS : dropping duplicates data, normalize data, handle null data into 0 for value column",
        )
        self.df.unpersist()
        return self

    def write_data(self, output_path: str):

        self.logger.info(f"Writing bronze layer data to {output_path}")
        try:
            if self.df.isEmpty():
                self.logger.warning("bronze DataFrame is empty. Bypassing write.")
                return self

            os.makedirs(output_path, exist_ok=True)

            Exists = os.path.exists(output_path)
            PartionData = any(
                file.endswith(".parquet") or file.startswith("event_date=")
                for file in os.listdir(output_path)
            )
            if Exists and PartionData:
                getRow = self.df.select("event_date").distinct().collect()

                getLateData = [
                    item["event_date"]
                    for item in getRow
                    if item["event_date"] is not None
                ]
                if getLateData:
                    self.logger.info(
                        f"Late Data Check: Fetching existing history for partitions: {getLateData}"
                    )
                    try:
                        existing_df = self.session.read.parquet(output_path).filter(
                            col("event_date").isin(getLateData)
                        )

                        combined_df = self.df.unionByName(
                            existing_df, allowMissingColumns=True
                        )

                        self.df = (
                            combined_df.withColumn(
                                "rn",
                                row_number().over(
                                    AntiDuplicate("event_id", "event_ts", "value")
                                ),
                            )
                            .filter(col("rn") == 1)
                            .drop("rn")
                        )

                        self.logger.info(
                            "Successfully merged late data with existing historical data."
                        )

                    except Exception as e:
                        self.logger.warning(
                            f"Could not read existing historical data (might be empty/corrupt): {e}"
                        )

            self.df.write.mode("overwrite").partitionBy("event_date").parquet(
                output_path
            )

            self.logger.info("SUCCESS: bronze layer data written idempotently.")

        except Exception as e:
            self.logger.error(f"Failed to write bronze data: {e}")

        return self
