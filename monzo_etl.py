from azure.storage.blob import BlobServiceClient
from pyspark.sql import functions as F


def get_blobs(connection_string: str) -> list:
    blob_service_client = BlobServiceClient.from_connection_string(
        connection_string
    )

    container_blobs = []

    for container in blob_service_client.list_containers():
        container_client = blob_service_client.get_container_client(
            container.name
        )
        container_blobs.extend(container_client.list_blobs())

    return container_blobs


def return_names(blobs) -> list:
    return [blob.name for blob in blobs]


def read_business_df(
    spark,
    storage_account: str,
    container: str,
    storage_key: str,
):
    container_path = (
        f"wasbs://{container}@"
        f"{storage_account}.blob.core.windows.net/"
    )

    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option(
            f"fs.azure.account.key."
            f"{storage_account}.blob.core.windows.net",
            storage_key,
        )
        .load(container_path)
    )


def transform_monzo_data(dataframe):
    columns_to_drop = [
        "Emoji",
        "Transaction ID",
        "Currency",
        "Local amount",
        "Local currency",
        "Notes and #tags",
        "Time",
        "Receipt",
        "Address",
        "Balance currency",
        "Category split",
    ]

    df = dataframe.drop(*columns_to_drop)

    df = df.na.fill(
        value=0,
        subset=["Money Out", "Money In"],
    )

    df = df.withColumn(
        "Name",
        F.when(
            F.col("Name").isin("OnlyFans", "OnlyFans H"),
            "Adult",
        ).otherwise(F.col("Name")),
    )

    return df
