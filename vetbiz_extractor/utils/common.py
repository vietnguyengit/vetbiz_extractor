import time
import pandas as pd
import pymysql
import pymssql
import calendar
import os
from datetime import datetime
from typing import List, Tuple, Union, Callable, Any, Optional, Dict


def measure_execution_time(script_function: Callable[..., Any]) -> Callable[..., Any]:
    """
    Decorator to measure the execution time of a script function and print the duration in minutes.

    :param script_function: The function representing the script to be timed.
    :return: The wrapper function.
    """

    def wrapper(*args, **kwargs) -> Any:
        start_time = time.time()
        print("Starting script...")
        # Run the script function
        result = script_function(*args, **kwargs)
        end_time = time.time()
        duration_seconds = end_time - start_time
        duration_minutes = duration_seconds / 60
        print(f"Script finished in {duration_minutes:.2f} minutes")
        return result

    return wrapper


def fetch_xero_journals_data_from_etani(
    db_server: str,
    db_user: str,
    db_password: str,
    db_name: str,
    journals_tables_list: List[str],
    batch_size: int = 10000,
    query_limit: Optional[int] = None,
) -> pd.DataFrame:
    """
    Fetches data from multiple Xero journals tables in the Etani SQL database and combines them into a single DataFrame.

    :param db_server: The database server address.
    :param db_user: The username for the database.
    :param db_password: The password for the database user.
    :param db_name: The name of the database.
    :param journals_tables_list: A list of journal table names to fetch data from.
    :param batch_size: Number of rows to fetch per batch
    :param query_limit: An optional limit on the number of rows per table.
    :return: A pandas DataFrame containing the combined data from the specified journal tables.
    """

    try:
        with pymssql.connect(
            server=db_server,
            user=db_user,
            password=db_password,
            database=db_name,
        ) as conn:
            cursor = conn.cursor()

            all_journals_data = []
            for journal_table in journals_tables_list:

                if query_limit:
                    query = f"SELECT TOP {query_limit} * FROM {journal_table};"
                else:
                    query = f"SELECT * FROM {journal_table};"

                cursor.execute(query)

                # Initialize an empty DataFrame to accumulate the results
                df = pd.DataFrame()
                column_names = [desc[0] for desc in cursor.description]

                while True:
                    rows = cursor.fetchmany(batch_size)
                    if not rows:
                        break

                    batch_df = pd.DataFrame(rows, columns=column_names)
                    batch_df = exclude_all_na_columns(batch_df)
                    df = pd.concat([df, batch_df], ignore_index=True)

                all_journals_data.append(df)

            cursor.close()

            results = pd.concat(all_journals_data)
            return results if not results.empty else pd.DataFrame(columns=column_names)
    except pymssql.DatabaseError as e:
        print(f"Database error occurred: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


def exclude_all_na_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Exclude columns that are all-NA from the DataFrame."""
    return df.dropna(axis=1, how="all")


def fetch_data_in_batches(
    query: str,
    db_user: str,
    db_password: str,
    db_host: str,
    db_name: str,
    db_port: int = 3306,
    batch_size: int = 10000,
) -> pd.DataFrame:
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
    try:
        # Using a context manager to connect to the database
        with pymysql.connect(
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
            database=db_name,
        ) as conn:
            cursor = conn.cursor()
            cursor.execute(query)

            # Initialize an empty DataFrame to accumulate the results
            df = pd.DataFrame()
            column_names = [desc[0] for desc in cursor.description]

            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break

                batch_df = pd.DataFrame(rows, columns=column_names)
                batch_df = exclude_all_na_columns(batch_df)
                df = pd.concat([df, batch_df], ignore_index=True)

            cursor.close()
            return df if not df.empty else pd.DataFrame(columns=column_names)
    except pymysql.MySQLError as e:
        print(f"Database error occurred: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return pd.DataFrame()


def end_of_month(year: int, month: int) -> int:
    """
    Return the last day of a given month in a given year.

    :param year: The year (four-digit integer)
    :param month: The month (integer between 1 and 12)
    :return: The last day of the month (integer)
    :raises ValueError: If the month is not between 1 and 12
    """
    if not (1 <= month <= 12):
        raise ValueError("Month must be between 1 and 12.")

    return calendar.monthrange(year, month)[1]


def validate_env_vars():
    required_vars = [
        "DB_USER",
        "DB_PASSWORD",
        "DB_HOST",
        "DB_NAME",
        "ETANI_DB_USER",
        "ETANI_DB_PASSWORD",
        "ETANI_DB_SERVER",
        "ETANI_DB_NAME",
    ]
    for var in required_vars:
        if os.getenv(var) is None:
            print(f"Error: Missing required environment variable {var}")
            return False
    return True


def validate_queries(queries: Dict[str, Any]) -> bool:
    """
    Validate the presence of required queries in the provided dictionary.

    :param queries: Dictionary containing query data.
    :return: True if all required queries are present, False otherwise.
    """
    required_queries = {"sales_query", "customers_query"}

    missing_queries = required_queries - queries.keys()

    if missing_queries:
        for missing_query in missing_queries:
            print(f"Error: Missing required query '{missing_query}' in queries.json")
        return False

    return True


def get_products_list(products: List[str], keyword: str) -> List[str]:
    """
    Filter the products based on the presence of a keyword.

    :param products: List of product names (strings).
    :param keyword: Keyword to search for within product names.
    :return: List of products containing the keyword (case-insensitively).
    """
    keyword_lower = keyword.lower()
    return [
        product
        for product in products
        if isinstance(product, str) and keyword_lower in product.lower()
    ]


def get_date_range_for_month(year: int, month: int) -> Tuple[datetime, datetime]:
    """
    Returns the start and end date for a given month and year.

    :param year: The year (four-digit integer)
    :param month: The month (integer between 1 and 12)
    :return: A tuple containing the start and end dates of the month
    """
    start_date = datetime(year, month, 1)
    end_date = datetime(year, month, calendar.monthrange(year, month)[1])
    return start_date, end_date


def filter_data_for_date_range(
    df: pd.DataFrame, start_date: Union[str, datetime], end_date: Union[str, datetime]
) -> pd.DataFrame:
    """
    Filters the DataFrame for entries within the specified date range.

    :param df: The input DataFrame containing a 'date_field' column.
    :param start_date: The start date of the date range (string or datetime).
    :param end_date: The end date of the date range (string or datetime).
    :return: A DataFrame filtered for the specified date range.
    :raises ValueError: If 'start_date' or 'end_date' are not valid date formats.
    """
    if isinstance(start_date, str):
        try:
            start_date = pd.to_datetime(start_date)
        except ValueError:
            raise ValueError("The provided start_date is not a valid date string")

    if isinstance(end_date, str):
        try:
            end_date = pd.to_datetime(end_date)
        except ValueError:
            raise ValueError("The provided end_date is not a valid date string")

    return df[(df["date_field"] >= start_date) & (df["date_field"] <= end_date)]
