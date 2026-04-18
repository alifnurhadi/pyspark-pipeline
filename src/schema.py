from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

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
