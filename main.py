from wal_records_processor import WalRecordsProcessor
from pathlib import Path


wal_service = WalRecordsProcessor()
wal_service.create_destination_table()
wal_service.load(Path("./wal.json"))
wal_service.merge_insertable_rows()
wal_service.write_to_database()

# print(wal_service.transaction_rows)
