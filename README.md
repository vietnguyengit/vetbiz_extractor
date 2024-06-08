
# VetBiz Data Extraction

This repository uses `Poetry` for dependency management.

## Prerequisites

- Miniconda
- Python 3.9

## Installation

### Step 1: Install Miniconda

If you don't already have Miniconda installed, you can download and install it from the [Miniconda website](https://docs.conda.io/en/latest/miniconda.html).

### Step 2: Create a Virtual Environment

Create a new virtual environment using Python 3.9:

```sh
conda create -n vetbiz-env python=3.9
```

Activate the virtual environment:

```sh
conda activate vetbiz-env
```

### Step 3: Install Poetry

Install Poetry if you don't already have it:

```sh
pip install poetry
```

Navigate to the project directory and install the dependencies:

```sh
poetry install
```

### Step 4: Prepare the Environment

Create a `.env` file in the project's root directory and add your database credentials:

```dotenv
DB_USER=your_db_user
DB_PASSWORD=your_db_password
DB_HOST=your_db_host
DB_PORT=your_db_port
DB_NAME=your_db_name
```

Make sure to replace `your_db_user`, `your_db_password`, `your_db_host`, `your_db_port`, and `your_db_name` with the data warehouse actual credentials.

### Step 5: Define Queries

Queries are defined in `queries.json` file in the project's root directory. The file should look like this:

```json
{
  "sales_query": "SELECT ...",
  "customers_query": "SELECT ..."
}
```

Replace the placeholder text with your actual SQL queries.

## Running the Application

### Without Limit Parameter

To run the application without limiting the number of records fetched:

```sh
poetry run python main.py
```

### With Limit Parameter

To run the application with a limit on the number of records fetched, use the `--limit` parameter:

```sh
poetry run python main.py --limit 100
```

Replace `100` with the desired limit on the number of records.

## Additional Information

- The environment variables for database credentials are validated at runtime.
- The queries from `queries.json` are also validated, and the application will terminate if they are not correctly defined.
