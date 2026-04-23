from pyspark.sql.functions import col
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.sql.window import Window

EVENT_schema = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("event_ts", TimestampType(), True),
        StructField("value", DoubleType(), True),
        StructField("_corrupt_record", StringType(), True),
    ]
)


def AntiDuplicate(partition: str, ordered: str, value: str):
    window_spec = Window.partitionBy(partition).orderBy(
        col(ordered).desc(), col(value).desc()
    )
    return window_spec
