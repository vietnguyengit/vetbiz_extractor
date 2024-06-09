
# VetBiz Data Extraction Library

This repository provides a library for the VetBiz PowerBI dashboard's Python scripts.

## Development

### Requirements:

- Python 3.9 or above
- Poetry

#### Installing dependencies and `vetbiz_extractor` module

```bash
pip install poetry
poetry install
```

#### Building wheel file

```bash
poetry build
```

Built wheel file is located in `dist` folder

## Usage

### Importing methods

```python
from vetbiz_extractor.utils.common import (
    fetch_data_in_batches,
    fetch_xero_journals_data_from_etani,
)
from vetbiz_extractor.core.insights_extractor import (
    get_follow_up_consults,
    get_dental_sales_after_consultation,
    get_lapsed_clients,
    get_filtered_active_customers,
)
```

### Fetching data from database sources

#### Xero journals from Etani database

```python
# Etani database connection details
etani_db_user = "..."
etani_db_password = "..."
etani_db_server = "..."
etani_db_name = "TAZTECH_d434a453-c859-41d1-93b7-f367a1572792"

journals_table_names = [
  "TAZTECH_CLIENT3_XEROBLUE_Journals",
  "TAZTECH_CLIENT4_XEROBLUE_Journals",
  "TAZTECH_CLIENT5_XEROBLUE_Journals",
  "TAZTECH_CLIENT6_XEROBLUE_Journals",
  "TAZTECH_CLIENT7_XEROBLUE_Journals",
  "TAZTECH_CLIENT8_XEROBLUE_Journals",
  "TAZTECH_CLIENT9_XEROBLUE_Journals",
]

A_Journals = fetch_xero_journals_data_from_etani(
  etani_db_server,
  etani_db_user,
  etani_db_password,
  etani_db_name,
  journals_table_names,
)
```

#### VetBiz data warehouse credentials declaration

```python
# VetBiz data warehouse database connection details
db_user = "..."
db_password = "..."
db_host = "..."
db_name = "..."
```

#### Sales data from `f_sales` table

`sales_query`

```sql
SELECT s.*, 
       p.practice_name, 
       c.clinic_name, 
       cu.customer_id, 
       cu.name,
       d.date_field AS invoice_date, 
       (s.unit_cost + s.fixed_cost) AS total_cost, 
       (s.unit_sale + s.fixed_sale) AS total_sale, 
       d.year, 
       d.month, 
       d.month_desc, 
       d.month_short_desc,
       pr.product_name 
FROM f_sales s
LEFT JOIN d_practice p ON p.practice_tk = s.practice_tk
LEFT JOIN d_clinic c ON c.clinic_tk = s.clinic_tk
LEFT JOIN d_customer cu ON cu.customer_tk = s.customer_tk
LEFT JOIN d_product pr ON pr.product_tk = s.product_tk
LEFT JOIN d_date d ON d.date_tk = s.invoice_date_tk;
```

```python
sales_data = fetch_data_in_batches(
    query=sales_query,
    db_host=db_host,
    db_user=db_user,
    db_password=db_password,
    db_name=db_name,
)
```

##### Customers data from `d_customer` table

`customers_query`

```sql
SELECT cu.*, d.date_field, d.year, d.month, d.month_desc, d.month_short_desc, p.practice_name, c.clinic_name 
FROM d_customer cu
LEFT JOIN d_date d ON d.date_tk = cu.created_tk
LEFT JOIN d_practice p ON p.practice_tk = cu.practice_tk
LEFT JOIN d_clinic c ON c.clinic_tk = cu.clinic_tk
WHERE cu.active = 1 AND cu.created_tk IS NOT NULL;
```

```python
customers_data = fetch_data_in_batches(
    query=customers_query,
    db_host=db_host,
    db_user=db_user,
    db_password=db_password,
    db_name=db_name,
)
```

Customers from `f_sales` table

`customers_from_sales_data_query`

```sql
SELECT s.visit_id, s.sale_id, s.clinic_tk, cus.customer_tk, cus.customer_id, cus.active,
pat.patient_id, p.practice_name, c.clinic_name, u.user_name, pr.product_name,
s.unit_cost, s.fixed_cost, s.unit_sale, s.fixed_sale, d.date_tk, d.date_field,
d.month, d.year, u.clinician
FROM f_sales s
JOIN d_user u ON u.user_tk = s.user_tk
JOIN d_customer cus ON cus.customer_tk = s.customer_tk
JOIN d_clinic c ON c.clinic_tk = s.clinic_tk
JOIN d_practice p ON p.practice_tk = c.practice_tk
JOIN d_product pr ON pr.product_tk = s.product_tk
JOIN d_date d ON d.date_tk = s.invoice_date_tk
JOIN d_patient pat ON pat.patient_tk = s.patient_tk
```

```python
# Get filtered active customers
customers_from_sales_data = fetch_data_in_batches(
    query=customers_from_sales_data_query,
    db_host=db_host,
    db_user=db_user,
    db_password=db_password,
    db_name=db_name,
)
```

### Extracting business insights

#### Follow-up consults within a specified days threshold

```python
# requires sales data as input
data = get_follow_up_consults(sales_data)
```

#### Dental sales made after consultations within a specified days threshold

```python
# requires sales data as input
data = get_dental_sales_after_consultation(sales_data)
```

#### Lapsed clients from the sales data

```python
# requires sales data as input
data = get_lapsed_clients(sales_data)
```

#### Active customers within a specified number of months.

```python
# requires customers from sales data as input
data = get_filtered_active_customers(customers_from_sales_data)
```

## API Reference

### fetch_xero_journals_data_from_etani

Fetches data from multiple Xero journals tables in the Etani SQL database and combines them into a single DataFrame.

**Parameters:**
- `db_server (str)`: The Etani database server address.
- `db_user (str)`: The username for the Etani's database.
- `db_password (str)`: The password for the Etani's database user.
- `db_name (str)`: The name of the Etani's database.
- `journals_tables_list (List[str])`: A list of journal table names to fetch data from.
- `batch_size (int)`: An batch size for fetching data.
- `query_limit (Optional[int])`: An optional limit on the number of rows per table.

**Returns:**
- `pd.DataFrame`: A DataFrame containing the combined data from the specified journal tables.

### fetch_data_in_batches

Fetch data from the database in batches.

**Parameters:**
- `query (str)`: SQL query to execute.
- `db_user (str)`: Database user.
- `db_password (str)`: Database password.
- `db_host (str)`: Database host.
- `db_port (int)`: Database port (default is 3306).
- `db_name (str)`: Database name.
- `batch_size (int)`: Number of rows to fetch per batch (default is 10000).

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
- `start_year (int)`: integer representing the start year from which to measure inactivity (default is 2018).

**Returns:**
- `pd.DataFrame`: A DataFrame filtered to include only the lapsed clients who have not made a purchase since the start year.

### get_filtered_active_customers

Filter the customers who have been active within a specified number of months since a given start year.

**Parameters:**
- `customers_from_sales_data_df (pd.DataFrame)`: DataFrame containing customer sales data. It must include a 'last_purchase_date' column.
- `start_year (int)`: integer representing the start year from which to measure activity (default is 2020).
- `months_threshold (int)`: Number of months within which a customer must have made a purchase to be considered active (default is 18 months).

**Returns:**
- `pd.DataFrame`: A DataFrame filtered to include only the active customers.

