
# VetBiz Data Extraction Library

This repository provides a library for the VetBiz PowerBI dashboard's Python scripts.

## API Reference

### fetch_xero_journals_data_from_etani

Fetches data from multiple Xero journals tables in the Etani SQL database and combines them into a single DataFrame.

**Parameters:**
- `db_server (str)`: The database server address.
- `db_user (str)`: The username for the database.
- `db_password (str)`: The password for the database user.
- `db_name (str)`: The name of the database.
- `journals_tables_list (List[str])`: A list of journal table names to fetch data from.
- `query_limit (Optional[int])`: An optional limit on the number of rows per table.
- `batch_size (Optional[int])`: An optional batch size for fetching data.

**Returns:**
- `pd.DataFrame`: A DataFrame containing the combined data from the specified journal tables.

### fetch_data_in_batches

Fetch data from the database in batches.

**Parameters:**
- `query (str)`: SQL query to execute.
- `db_user (str)`: Database user.
- `db_password (str)`: Database password.
- `db_host (str)`: Database host.
- `db_port (Optional[int])`: Database port (default is 3306).
- `db_name (str)`: Database name.
- `batch_size (Optional[int])`: Number of rows to fetch per batch (default is 10000).

**Returns:**
- `pd.DataFrame`: A DataFrame with the fetched data.

### get_follow_up_consults

Filter the sales data to retrieve follow-up consults within a specified days threshold.

**Parameters:**
- `sales_data (pd.DataFrame)`: DataFrame containing sales data.
- `days_threshold (int)`: Number of days to define the follow-up threshold (default is 14 days).

**Returns:**
- `pd.DataFrame`: DataFrame filtered for follow-up consults within the specified days threshold.

### get_dental_sales_after_consultation

Filter the sales data to retrieve dental sales made after consultations within a specified days threshold.

**Parameters:**
- `sales_data (pd.DataFrame)`: DataFrame containing sales data.
- `days_threshold (int)`: Number of days to define the threshold for sales after consultation (default is 14 days).

**Returns:**
- `pd.DataFrame`: DataFrame filtered for dental sales made after consultations within the specified days threshold.

### get_lapsed_clients

Identify and filter lapsed clients from the sales data. A lapsed client is defined as a client who has not made any purchases since a specified start year.

**Parameters:**
- `sales_data (pd.DataFrame)`: Pandas DataFrame containing sales data.
  It must include a 'last_purchase_date' column with dates of the last purchase.
- `start_year (Optional[int])`: Optional integer representing the start year from which to measure inactivity (default is 2018).

**Returns:**
- `pd.DataFrame`: A DataFrame filtered to include only the lapsed clients who have not made a purchase since the start year.

### get_filtered_active_customers

Filter the customers who have been active within a specified number of months since a given start year.

**Parameters:**
- `customers_from_sales_data_df (pd.DataFrame)`: DataFrame containing customer sales data. It must include a 'last_purchase_date' column.
- `start_year (Optional[int])`: Optional integer representing the start year from which to measure activity (default is 2020).
- `months_threshold (int)`: Number of months within which a customer must have made a purchase to be considered active (default is 18 months).

**Returns:**
- `pd.DataFrame`: A DataFrame filtered to include only the active customers.

