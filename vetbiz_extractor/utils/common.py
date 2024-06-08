import time
import pandas as pd
import pymysql
import calendar
import os


def measure_execution_time(script_function):
    """
    Decorator to measure the execution time of a script function and print the duration in minutes.

    :param script_function: The function representing the script to be timed.
    :return: The wrapper function.
    """

    def wrapper():
        start_time = time.time()
        print("Starting script...")

        # Run the script function
        result = script_function()

        end_time = time.time()
        duration_seconds = end_time - start_time
        duration_minutes = duration_seconds / 60

        print(f"Script finished in {duration_minutes:.2f} minutes")

        return result

    return wrapper


def fetch_data_in_batches(
    query, db_user, db_password, db_host, db_name, db_port=3306, batch_size=10000
):
    """
    Fetch data from the database in batches.

    :param query: SQL query to execute
    :param db_user: Database user
    :param db_password: Database password
    :param db_host: Database host
    :param db_port: Database port
    :param db_name: Database name
    :param batch_size: Number of rows to fetch per batch
    :return: DataFrame with the fetched data
    """

    def exclude_all_na_columns(df):
        """Exclude columns that are all-NA from the DataFrame."""
        return df.dropna(axis=1, how="all")

    # Connect to the MySQL database
    conn = pymysql.connect(
        user=db_user, password=db_password, host=db_host, port=db_port, database=db_name
    )

    # Create a cursor object
    cursor = conn.cursor()

    # Execute the query
    cursor.execute(query)

    # Fetch and process rows in batches
    rows = cursor.fetchmany(batch_size)
    column_names = [desc[0] for desc in cursor.description]

    # Initialize an empty DataFrame to accumulate the results
    df = pd.DataFrame(columns=column_names)

    while rows:
        # Append the current batch to the DataFrame
        batch_df = pd.DataFrame(rows, columns=column_names)

        # Exclude all-NA columns from the batch DataFrame
        batch_df = exclude_all_na_columns(batch_df)

        # Exclude all-NA columns from the accumulated DataFrame
        df = exclude_all_na_columns(df)

        # Concatenate the current batch to the accumulated DataFrame
        df = pd.concat([df, batch_df], ignore_index=True)

        # Fetch the next batch of rows
        rows = cursor.fetchmany(batch_size)

    # Close the cursor and connection
    cursor.close()
    conn.close()

    return df


def end_of_month(year, month):
    return calendar.monthrange(year, month)[1]


def validate_env_vars():
    required_vars = ["DB_USER", "DB_PASSWORD", "DB_HOST", "DB_NAME"]
    for var in required_vars:
        if os.getenv(var) is None:
            print(f"Error: Missing required environment variable {var}")
            return False
    return True


def validate_queries(queries):
    required_queries = ["sales_query", "customers_query"]
    for query in required_queries:
        if query not in queries:
            print(f"Error: Missing required query '{query}' in queries.json")
            return False
    return True


def get_products_list(products, keyword):
    return [
        product
        for product in products
        if isinstance(product, str) and keyword in product.lower()
    ]
