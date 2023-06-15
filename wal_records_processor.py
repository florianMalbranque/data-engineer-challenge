from dataclasses import dataclass, field, fields, astuple
from typing import List, Dict
from pathlib import Path
import json
import logging
import sys
from abc import abstractmethod
from sqlite_client import SQLiteClient


@dataclass
class WALRow:
    """
    Base class representing a row in the Write-Ahead Log (WAL).
    """

    kind: int
    schema: int
    table: int
    column_names: List[int]
    column_types: List[int]
    column_values: List[int]
    column_names_values: Dict[int, int | int | Dict]

    @classmethod
    def from_dict(cls, data):
        """
        Creates a WALRow instance from a dictionary.
        """
        return cls(
            data["kind"],
            data["schema"],
            data["table"],
            data["columnnames"],
            data["columntypes"],
            data["columnvalues"],
            dict(
                zip(
                    data["columnnames"],
                    [
                        cls.transform_to_dict_if_json(value)
                        for value in data["columnvalues"]
                    ],
                )
            ),
        )

    @staticmethod
    def transform_to_dict_if_json(value):
        """
        Transforms the value to a dictionary if it's a valid JSON string.
        """
        try:
            # Try parsing as JSON
            value = json.loads(value)
        except (json.JSONDecodeError, TypeError):
            value = value

        return value

    @abstractmethod
    def get_join_key(self, data) -> int:
        """
        Abstract method to get the join key for the row.
        """
        raise NotImplementedError

    @abstractmethod
    def get_insertable_values(self) -> int:
        """
        Abstract method to get the insertable values for the row.
        """
        raise NotImplementedError


@dataclass
class EventV2DataRow(WALRow):
    """
    Class representing an event_v2_data row in the WAL.
    """

    def get_insertable_values(self) -> Dict[str, int]:
        """
        Returns the insertable values for the row as a dictionary.
        """
        return {
            key.split(".")[-1]: self.column_names_values.get(key)
            for key in [
                "event_id",
                "flow_id",
                "created_at",
                "transaction_lifecycle_event",
                "error_details.decline_reason",
                "error_details.decline_type",
            ]
        }


@dataclass
class TransactionsRow(WALRow):
    """
    Class representing a transaction row in the WAL.
    """

    def get_join_key(self) -> int:
        """
        Returns the join key for the row.
        """
        value_index = self.column_names.index("transaction_id")
        if value_index is not None:
            return self.column_values[value_index]

    def get_insertable_values(self) -> Dict[str, int]:
        """
        Returns the insertable values for the row as a dictionary.
        """
        return {
            key.split(".")[-1]: self.column_names_values.get(key)
            for key in [
                "transaction_id",
                "transaction_type",
                "created_at",
                "amount",
                "currency_code",
                "processor_merchant_account_id",
            ]
        }


@dataclass
class TransactionRequestsRow(WALRow):
    """
    Class representing a transaction_request row in the WAL.
    """

    def get_join_key(self) -> int:
        """
        Returns the join key for the row.
        """
        value_index = self.column_names.index("flow_id")
        if value_index is not None:
            return self.column_values[value_index]

    def get_insertable_values(self) -> Dict[str, int]:
        """
        Returns the insertable values for the row as a dictionary.
        """
        return {
            key.split(".")[-1]: self.column_names_values.get(key)
            for key in ["vault_options.payment_method"]
        }


@dataclass
class PaymentInstrumentTokenDataRow(WALRow):
    """
    Class representing a payment_instrument_token_data row in the WAL.
    """

    def get_join_key(self) -> int:
        """
        Returns the join key for the row.
        """
        value_index = self.column_names.index("token_id")
        if value_index is not None:
            return self.column_values[value_index]

    def get_insertable_values(self) -> Dict[str, int]:
        """
        Returns the insertable values for the row as a dictionary.
        """
        return {
            key.split(".")[-1]: self.column_names_values.get(key)
            for key in [
                "three_d_secure_authentication",
                "payment_instrument_type",
                "vault_data.customer_id",
            ]
        }


@dataclass
class InsertableRow:
    """
    Class representing an insertable row in the destination table.
    """

    event_id: int = None
    flow_id: int = None
    created_at: str = None
    transaction_lifecycle_event: str = None
    decline_reason: str = None
    decline_type: str = None
    payment_method: str = None
    transaction_id: int = None
    transaction_type: str = None
    amount: int = None
    currency_code: str = None
    processor_merchant_account_id: int = None
    three_d_secure_authentication: str = None
    payment_instrument_type: str = None
    customer_id: int = None


@dataclass
class WalRecordsProcessor:
    """
    Class used for processing WAL records.
    """

    logger = logging.getLogger("WalRecordProcessor")
    logger.addHandler(logging.StreamHandler(sys.stdout))
    logger.setLevel(logging.INFO)
    sQLiteClient = SQLiteClient("./db")
    sQLiteClient.connect()

    event_v2_data_rows: List[EventV2DataRow] = field(default_factory=list)
    transaction_rows: Dict[int, TransactionsRow] = field(default_factory=dict)
    transaction_request_rows: Dict[int, TransactionRequestsRow] = field(
        default_factory=dict
    )
    payment_instrument_token_data_rows: Dict[
        int, PaymentInstrumentTokenDataRow
    ] = field(default_factory=dict)
    insertable_rows: List[InsertableRow] = field(default_factory=list)

    def load(self, path: Path):
        """
        Loads WAL records from a JSON file.
        """
        self.logger.debug("Loading json file.")
        with open(path) as file:
            data = json.load(file)
        for row in data:
            row = row["change"][0]
            match row["table"]:
                case "event_v2_data":
                    self.event_v2_data_rows.append(EventV2DataRow.from_dict(row))
                case "transaction":
                    transaction_row = TransactionsRow.from_dict(row)
                    self.transaction_rows[
                        transaction_row.get_join_key()
                    ] = transaction_row
                case "transaction_request":
                    transaction_request_row = TransactionRequestsRow.from_dict(row)
                    self.transaction_request_rows[
                        transaction_request_row.get_join_key()
                    ] = transaction_request_row

                case "payment_instrument_token_data":
                    payment_instrument_token_data_row = (
                        PaymentInstrumentTokenDataRow.from_dict(row)
                    )
                    self.payment_instrument_token_data_rows[
                        payment_instrument_token_data_row.get_join_key()
                    ] = payment_instrument_token_data_row
                case _:
                    self.logger.warning(f"Unknown table: {row['table']}")

    def merge_insertable_rows(self):
        """
        Merges the insertable rows from different tables into a single insertable row.
        """
        for event_v2_data_row in self.event_v2_data_rows:
            merged_row = event_v2_data_row.get_insertable_values()
            transaction_row = self.transaction_rows.get(
                event_v2_data_row.column_names_values.get("transaction_id")
            )
            if transaction_row:
                merged_row.update(transaction_row.get_insertable_values())
            transaction_request_row = self.transaction_request_rows.get(
                event_v2_data_row.column_names_values.get("flow_id")
            )
            if transaction_request_row is not None:
                merged_row.update(transaction_request_row.get_insertable_values())
            payment_instrument_token_data_row = (
                self.payment_instrument_token_data_rows.get(
                    transaction_request_row.column_names_values.get("token_id")
                )
                if transaction_request_row is not None
                else None
            )

            if payment_instrument_token_data_row is not None:
                merged_row.update(
                    payment_instrument_token_data_row.get_insertable_values()
                )
            self.insertable_rows.append(InsertableRow(**merged_row))

    def create_destination_table(self):
        """
        Creates the destination table in the SQLite database based on the InsertableRow class.
        """
        cls = InsertableRow
        annotations = getattr(cls, "__annotations__", {})
        defaults = getattr(cls, "__dict__", {})
        column_definitions = []
        for field_name, field_type in annotations.items():
            default_value = defaults.get(field_name)
            column_definitions.append(
                f"{field_name} {self.convert_python_type_to_sqlite(field_type)} {default_value if default_value else 'NULL'}"
            )
        column_definitions_str = ", ".join(column_definitions)

        self.sQLiteClient.create_table("merged_wal_records", column_definitions_str)

    def convert_python_type_to_sqlite(self, python_type):
        """
        Converts a Python type to the corresponding SQLite type.
        """
        type_mapping = {
            int: "INTEGER",
            float: "REAL",
            str: "TEXT",
            bytes: "BLOB",
            bool: "INTEGER",
            list: "TEXT",
            dict: "TEXT",
            tuple: "TEXT",
        }
        return type_mapping.get(python_type, "TEXT")

    def write_to_database(self):
        """
        Writes the insertable rows to the destination table in the SQLite database.
        """
        column_names = [field.name for field in fields(InsertableRow)]
        column_values = [astuple(row) for row in self.insertable_rows]
        self.sQLiteClient.batch_insert(
            "merged_wal_records", column_names, column_values
        )
