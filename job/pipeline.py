import argparse
import logging
import os

import yaml
from pyspark.sql import SparkSession

from src.bronzeLayer import BronzeLayer
from src.goldLayer import GoldLayer

# Import your layers and schema
from src.schema import EVENT_schema
from src.silverLayer import SilverLayer


def load_config(path: str) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)


def createSession(app_name: str = "de-pipeline", treshold_join=15) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master("local")
        .config("spark.sql.autoBroadcastJoinThreshold", treshold_join * 1024 * 1024)
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )


def autoCreate_output(basePath: str, output: str) -> str:

    joinedPath = os.path.join(basePath, output)

    os.makedirs(joinedPath, exist_ok=True)

    return joinedPath


def main(config_path: str):

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - PIPELINE - %(levelname)s - %(message)s",
    )
    logger = logging.getLogger("Orchestrator")

    try:
        config = load_config(config_path)
        spark = createSession()

        raw_events_path = config["paths"]["raw_events"]
        users_ref_path = config["paths"]["users"]
        output_base_path = config["paths"]["output"]
        quarentine_path = config["paths"]["quarantine"]

        logger.info(">>> STARTING BRONZE LAYER")
        bronzepath = autoCreate_output(output_base_path, "bronze")
        bronze = BronzeLayer(spark, raw_events_path, EVENT_schema)
        bronze.readEvent().handleData(quarentine_path).write_data(bronzepath)

        silverpath = autoCreate_output(output_base_path, "silver")
        silver = SilverLayer(spark, bronze.df, users_ref_path)
        silver.BasicEnrichment().AdvancedEnrichment().write_data(silverpath)

        logger.info(">>> Processing Aggregation: Country DAU")

        gold_dau = GoldLayer(silver.df, spark)
        gold_dau.Agg_CountryDAU().write_data(
            autoCreate_output(output_base_path, "gold/gold_DAU")
        )

        logger.info(">>> Processing Aggregation: Avg Days Before Purchase")
        gold_avg = GoldLayer(silver.df, spark)
        gold_avg.Agg_AvgDaybeforePurchase().write_data(
            autoCreate_output(output_base_path, "gold/DaysBeforePurchase")
        )

        logger.info("Pipeline Executed complete")

    except Exception as e:
        logger.critical(f"Pipeline execution halted due to unrecoverable error: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()
    main(args.config)
