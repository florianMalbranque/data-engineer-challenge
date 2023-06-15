import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch, mock_open
import json
from wal_records_processor import (
    WalRecordsProcessor,
    InsertableRow,
    TransactionsRow,
    EventV2DataRow,
    TransactionRequestsRow,
    PaymentInstrumentTokenDataRow,
)


@pytest.fixture
def wal_records_processor():
    return WalRecordsProcessor()


@pytest.fixture
def insertable_row():
    return InsertableRow()


def test_merge_insertable_rows(wal_records_processor, insertable_row):
    # Add mock data to the processor
    wal_records_processor.event_v2_data_rows = [
        EventV2DataRow(
            kind=1,
            schema=1,
            table=1,
            column_names=["event_id", "flow_id", "transaction_id"],
            column_types=["int", "int", "int"],
            column_values=[1, 2, 2],
            column_names_values={"event_id": 1, "flow_id": 3, "transaction_id": 2},
        )
    ]
    wal_records_processor.transaction_rows = {
        2: TransactionsRow(
            kind=2,
            schema=2,
            table=2,
            column_names=["transaction_id", "transaction_type"],
            column_types=["int", "str"],
            column_values=[2, "sale"],
            column_names_values={"transaction_id": 2, "transaction_type": "sale"},
        )
    }
    wal_records_processor.transaction_request_rows = {
        3: TransactionRequestsRow(
            kind=3,
            schema=3,
            table=3,
            column_names=["flow_id", "vault_options.payment_method", "token_id"],
            column_types=["int", "str", "int"],
            column_values=[2, "credit_card", 4],
            column_names_values={
                "flow_id": 2,
                "vault_options.payment_method": "credit_card",
                "token_id": 4,
            },
        )
    }
    wal_records_processor.payment_instrument_token_data_rows = {
        4: PaymentInstrumentTokenDataRow(
            kind=4,
            schema=4,
            table=4,
            column_names=["token_id", "three_d_secure_authentication"],
            column_types=["int", "str"],
            column_values=[4, "authenticated"],
            column_names_values={
                "token_id": 4,
                "three_d_secure_authentication": "authenticated",
            },
        )
    }

    wal_records_processor.merge_insertable_rows()
    assert len(wal_records_processor.insertable_rows) == 1
    assert wal_records_processor.insertable_rows[0].event_id == 1
    assert wal_records_processor.insertable_rows[0].flow_id == 3
    assert wal_records_processor.insertable_rows[0].transaction_id == 2
    assert wal_records_processor.insertable_rows[0].transaction_type == "sale"
    assert wal_records_processor.insertable_rows[0].payment_method == "credit_card"
    assert (
        wal_records_processor.insertable_rows[0].three_d_secure_authentication
        == "authenticated"
    )


def test_create_destination_table(wal_records_processor):
    # Mock the SQLiteClient and connect method
    wal_records_processor.sQLiteClient = MagicMock()
    wal_records_processor.sQLiteClient.connect = MagicMock()

    # Call the create_destination_table() method
    wal_records_processor.create_destination_table()

    # Assert that the SQLiteClient's create_table method was called correctly
    wal_records_processor.sQLiteClient.create_table.assert_called_with(
        "merged_wal_records",
        "event_id INTEGER NULL, flow_id INTEGER NULL, created_at TEXT NULL, "
        "transaction_lifecycle_event TEXT NULL, decline_reason TEXT NULL, "
        "decline_type TEXT NULL, payment_method TEXT NULL, transaction_id INTEGER NULL, "
        "transaction_type TEXT NULL, amount INTEGER NULL, currency_code TEXT NULL, "
        "processor_merchant_account_id INTEGER NULL, "
        "three_d_secure_authentication TEXT NULL, payment_instrument_type TEXT NULL, "
        "customer_id INTEGER NULL",
    )


def test_write_to_database(wal_records_processor):
    # Mock the SQLiteClient and batch_insert method
    wal_records_processor.sQLiteClient = MagicMock()
    wal_records_processor.sQLiteClient.batch_insert = MagicMock()

    # Add mock insertable rows to the processor
    insertable_row_1 = InsertableRow(event_id=1, flow_id=2)
    insertable_row_2 = InsertableRow(event_id=3, flow_id=4)
    wal_records_processor.insertable_rows = [insertable_row_1, insertable_row_2]

    # Call the write_to_database() method
    wal_records_processor.write_to_database()

    # Assert that the SQLiteClient's batch_insert method was called correctly
    wal_records_processor.sQLiteClient.batch_insert.assert_called_with(
        "merged_wal_records",
        [
            "event_id",
            "flow_id",
            "created_at",
            "transaction_lifecycle_event",
            "decline_reason",
            "decline_type",
            "payment_method",
            "transaction_id",
            "transaction_type",
            "amount",
            "currency_code",
            "processor_merchant_account_id",
            "three_d_secure_authentication",
            "payment_instrument_type",
            "customer_id",
        ],
        [
            (
                1,
                2,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            ),
            (
                3,
                4,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            ),
        ],
    )
