# Data Engineering Challenge

In this challenge, we will simulate the processing of batches of Write-Ahead Log (WAL) records. These records contain valuable data that will be utilized to generate metrics for our customers on the dashboard. However, prior to utilizing the data, it must undergo preprocessing, merging, and insertion into a database. To simplify the task, we will employ SQLite, although in real-world scenarios, a more robust solution like Elasticsearch would be employed.

### WAL

A WAL record is created each time there is an insert, update, or delete in the database - we will only be dealing with insert records for this exercise. Each WAL record is represented by a dictionary in Python:

```
  ## INSERT
  {
    "change": [
      {
        "kind": "insert",
        "schema": "public",
        "table": "foo",
        "columnnames": ["a", "b", "c"],
        "columntypes": ["integer", "character varying(30)", "timestamp without time zone"],
        "columnvalues": [1, "Backup and Restore", "2018-03-27 12:05:29.914496"]
      }
    ]
  }

```
- The WAL records are produced by 4 tables called:
	- event_v2_data
	- transaction
	- transaction_request
	- payment_instrument_token_data

## The Task

The `wal.json` file consists of a collection of Write-Ahead Log (WAL) records. Our objective is to generate a dashboard metric by combining information from the `event_v2_data`, `transaction`, `transaction_request`, and `payment_instrument_token_data` records. The join conditions for the records are as follows:

```
  event_v2_data.transaction_id = transaction.transaction_id
  event_v2_data.flow_id = transaction_request.flow_id
  transaction_request.token_id = payment_instrument_token_data.token_id
```

After joining the records, we only need to retain a subset of the data to create the final metric, which should have the following structure:

```
	{
		"event_v2_data.event_id": "value",
		"event_v2_data.flow_id": "value",
		"event_v2_data.created_at": "value",
		"event_v2_data.transaction_lifecycle_event": "value",
		"event_v2_data.error_details.decline_reason": "value",
		"event_v2_data.error_details.decline_type": "value",
		"transaction_request.vault_options.payment_method": "value",
		"transaction.transaction_id": "value",
		"transaction.transaction_type": "value",
		"transaction.amount": "value",
		"transaction.currency_code": "value",
		"transaction.processor_merchant_account_id": "value",
		"payment_instrument_token_data.three_d_secure_authentication": "value",
		"payment_instrument_token_data.payment_instrument_type": "value",
		"payment_instrument_token_data.vault_data.customer_id": "value"
	}
```

To facilitate comprehension, we have included the table name along with each column name. However, when inserting the data into the database, please exclude the table name (e.g., use `payment_method` instead of `transaction_request.vault_options.payment_method`). You will need to create a table in SQLite to store the metric, and the WAL records contain all the necessary information, including the data type for each column.

Once the metric is created, it should be added to the database. By the end of the task, there should be 529 records in the database.

The challenge should be tackled using Python and only modules from the standard library. Avoid using third-party libraries like `pandas`. The file `main.py` provides some initial code to assist you, but you are free to keep or discard it as needed.

## What We're Looking For

☑️  Working code

☑️  Clean code

☑️  Good documentation

## Finishing Up 📈

📄 1. Document your findings, how did you go about solving the challenge?

☁️ 2. Push your work to your fork on GitHub

✉️ 3. Share both the code and your findings with us (please add the Github user primer-api as a reviewer) 🎉

We look forward to discussing your solution!
