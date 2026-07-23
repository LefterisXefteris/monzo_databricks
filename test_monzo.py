import sys

sys.path.insert(
    0,
    "/Workspace/Users/firomirokiro00@outlook.com/Drafts"
)

from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import SparkSession

from monzo_etl import (
    get_blobs,
    return_names,
    read_business_df,
    transform_monzo_data,
)


@pytest.fixture(scope="session")
def spark():
    spark_session = (
        SparkSession.builder
        .appName("monzo-pytest")
        .getOrCreate()
    )

    yield spark_session


@patch("monzo_etl.BlobServiceClient")
def test_get_blobs_returns_blobs(mock_blob_service_client):
    mock_service = MagicMock()

    mock_blob_service_client.from_connection_string.return_value = (
        mock_service
    )

    fake_container = MagicMock()
    fake_container.name = "monzobussiness"

    mock_service.list_containers.return_value = [
        fake_container
    ]

    fake_blob = MagicMock()
    fake_blob.name = "transactions.csv"

    fake_container_client = MagicMock()

    fake_container_client.list_blobs.return_value = [
        fake_blob
    ]

    mock_service.get_container_client.return_value = (
        fake_container_client
    )

    result = get_blobs("fake-connection-string")

    assert len(result) == 1
    assert result[0].name == "transactions.csv"
