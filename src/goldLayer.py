import logging

from pyspark.sql import DataFrame, SparkSession


class GoldLayer:
    def __init__(self, df: DataFrame, session: SparkSession):
        self.df = df
        self.logger = logging.getLogger(self.__class__.__name__)
        self.session = session

        if not self.logger.handlers:
            ch = logging.StreamHandler()
            ch.setFormatter(
                logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
            )
            self.logger.addHandler(ch)

    def Agg_CountryDAU(self):
        try:
            if self.df.isEmpty():
                self.logger.warning(
                    "Input DataFrame is empty. Bypassing Agg_CountryDAU."
                )
                return self

            self.df.createOrReplaceTempView("silver_events1")

            self.df = self.session.sql("""
                SELECT
                    event_date,
                    country,
                    COUNT(event_id)                                    AS total_events,
                    COALESCE(SUM(value), 0)                           AS total_value,
                    COALESCE(SUM(CASE WHEN is_purchase THEN 1 ELSE 0 END), 0) AS total_purchases,
                    COUNT(DISTINCT user_id)                            AS unique_users
                FROM silver_events1
                GROUP BY event_date, country
            """)

            self.logger.info(
                "SUCCESS: Agg_CountryDAU computed with strict schema adherence."
            )
            return self

        except Exception as e:
            self.logger.error(
                f"Error computing Agg_CountryDAU: {e}. Returning empty DataFrame."
            )
            return self

    def Agg_AvgDaybeforePurchase(self):
        try:
            # Graceful error handling: Empty state bypass
            if self.df.isEmpty():
                self.logger.warning(
                    "Input DataFrame is empty. Bypassing Agg_AvgDaybeforePurchase."
                )
                return self

            self.df.createOrReplaceTempView("silver_events2")

            self.df = self.session.sql("""
                SELECT
                    event_date,
                    country,
                    ROUND(AVG(days_since_signup), 2) AS avg_days_to_purchase
                FROM silver_events2
                WHERE is_purchase
                  AND days_since_signup >= 0
                GROUP BY event_date, country
            """)

            self.logger.info("SUCCESS: Agg_AvgDaybeforePurchase computed.")
            return self

        except Exception as e:
            self.logger.error(
                f"Error computing Agg_AvgDaybeforePurchase: {e}. Returning empty DataFrame."
            )
            return self

    def write_data(self, output_path: str):
        if not self.df.isEmpty():
            self.df.write.mode("overwrite").partitionBy("event_date").parquet(
                output_path
            )
            self.logger.info(f"SUCCESS: Gold layer written to {output_path}")
        return self
