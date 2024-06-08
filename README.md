
# VetBiz Data Extraction Library

This repository provides a library for the VetBiz PowerBI dashboard's Python scripts.

## I. Setup Essential Runtime Environment

To set up the essential runtime environment, follow the steps below:

### Step 1: Install Python 3.9 or above

Make sure you have Python 3.9 or later installed on your system to use this library.

### Step 2: Install Poetry

```sh
pip install poetry
```

### Step 3: Install Required Dependencies

Navigate to the project's root directory and install the dependencies specified in the `pyproject.toml` file by running:

```sh
poetry install
```

## II. Method Reference

### 1. fetch_data_in_batches

Fetch data from the database in batches.

#### Parameters

- **`query` (str):** The SQL query to fetch data.
- **`db_user` (str):** The database username.
- **`db_password` (str):** The database password.
- **`db_host` (str):** The database host.
- **`db_name` (str):** The database name.
- **`db_port` (int, optional):** The database port (default is 3306).
- **`batch_size` (int, optional):** The number of records to fetch per batch (default is 10,000).

#### Sample usage

```python
# Fetch sales data
df = fetch_data_in_batches(
    query="SELECT * FROM aTable WHERE ...",
    db_user="username",
    db_password="password",
    db_host=3306,
    db_name="sample_db"
)
```

### 2. get_follow_up_consults

Get follow up within 14 days from consults

#### Parameters

- **`sales_data` (Dataframe):** The aggregated sales data
- **`days_threshold` (int, optional):** The days threshold (default is 14).

#### Sample usage

```python
follow_up_df = get_follow_up_consults(df_sales_full)
```

### 3. get_dental_sales_after_consultation

Get dental sales within 14 days after consultation

#### Parameters

- **`sales_data` (Dataframe):** The aggregated sales data
- **`days_threshold` (int, optional):** The days threshold (default is 14).

#### Sample usage

```python
consults_to_dental_df = get_dental_sales_after_consultation(df_sales_full)
```

### 4. get_lapsed_clients

Get lapsed clients

#### Parameters

- **`sales_data` (Dataframe):** The aggregated sales data

#### Sample usage

```python
lapsed_clients_df = get_lapsed_clients(df_sales_full)
```

## III. Import methods

After installing this library, you would be able to import its methods.

```python
from vetbiz_extractor.utils.common import (
    fetch_data_in_batches
)

from vetbiz_extractor.core.insights_extractor import (
    get_lapsed_clients,
    get_follow_up_consults,
    get_dental_sales_after_consultation,
)
```

## IV. Queries

### Sales data

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

### Customers data

```sql
SELECT cu.*, d.date_field, d.year, d.month, d.month_desc, d.month_short_desc, p.practice_name, c.clinic_name 
FROM d_customer cu
LEFT JOIN d_date d ON d.date_tk = cu.created_tk
LEFT JOIN d_practice p ON p.practice_tk = cu.practice_tk
LEFT JOIN d_clinic c ON c.clinic_tk = cu.clinic_tk
WHERE cu.active = 1 AND cu.created_tk IS NOT NULL;
```
