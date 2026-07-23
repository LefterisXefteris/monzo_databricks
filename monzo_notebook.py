from pyspark.sql.functions import lit

from monzo_etl import read_business_df, transform_monzo_data


storage_account = "financelefterisdata"
business_container = "monzobussiness"
personal_container = "monzopersonal"
output_container = "monzobussinesconcat"

# Do not hard-code your real Azure key here.
# Read it from a Databricks secret scope instead.
storage_key = "<YOUR_STORAGE_KEY_FROM_DATABRICKS_SECRET>"


business_df = read_business_df(
    spark=spark,
    storage_account=storage_account,
    container=business_container,
    storage_key=storage_key,
)

business_df.show(1000)
business_df.printSchema()

business_df = transform_monzo_data(business_df)


personal_df = read_business_df(
    spark=spark,
    storage_account=storage_account,
    container=personal_container,
    storage_key=storage_key,
)

personal_df = transform_monzo_data(personal_df)

personal_df = personal_df.withColumn(
    "Balance",
    lit(0.0).cast("double")
)

personal_df.printSchema()


def unite_monzo_data(df1, df2):
    return df1.unionByName(df2).sort("Date")


united_df = unite_monzo_data(
    business_df,
    personal_df,
)

united_df.show(1000)
print("Rows:", united_df.count())


if united_df.count() > 0:
    output_path = (
        f"wasbs://{output_container}@"
        f"{storage_account}.blob.core.windows.net/"
        "processed/monzo_united.csv"
    )

    (
        united_df.coalesce(1)
        .write
        .option("header", "true")
        .mode("overwrite")
        .option(
            f"fs.azure.account.key."
            f"{storage_account}.blob.core.windows.net",
            storage_key,
        )
        .csv(output_path)
    )
else:
    print("united_df is empty. No data written to Azure Blob Storage.")
