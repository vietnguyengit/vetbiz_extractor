from vetbiz_extractor.utils.common import (
    measure_execution_time,
    validate_queries,
    validate_env_vars,
    fetch_data_in_batches,
    fetch_xero_journals_data_from_etani
)

from vetbiz_extractor.core.insights_extractor import (
    get_lapsed_clients,
    get_follow_up_consults,
    get_dental_sales_after_consultation,
    get_filtered_active_customers
)
import argparse
import os
import json
from dotenv import load_dotenv

load_dotenv()


@measure_execution_time
def main():
    # Validate environment variables
    if not validate_env_vars():
        print("Missing required environment variables. Terminating application.")
        return

    # Set up argument parser
    parser = argparse.ArgumentParser(description="Fetch data with optional limit.")
    parser.add_argument("--limit", type=int, help="Limit the number of records fetched")

    # Parse arguments
    args = parser.parse_args()
    query_limit = args.limit

    # Database connection details
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST")
    db_name = os.getenv("DB_NAME")

    # Read queries from the queries.json file
    try:
        with open("queries.json") as f:
            queries = json.load(f)
    except FileNotFoundError:
        print("Error: queries.json file not found. Terminating application.")
        return
    except json.JSONDecodeError:
        print("Error: queries.json file is not a valid JSON. Terminating application.")
        return

    # Validate queries
    if not validate_queries(queries):
        print("Missing required queries. Terminating application.")
        return

    sales_query = queries["sales_query"]
    customers_query = queries["customers_query"]
    customers_from_sales_data_query = queries["customers_from_sales_data_query"]

    # Add query limit if specified
    if query_limit is not None:
        sales_query += f" LIMIT {query_limit}"
        customers_query += f" LIMIT {query_limit}"
        customers_from_sales_data_query += f" LIMIT {query_limit}"

    # Fetch sales data
    df_sales_full = fetch_data_in_batches(
        query=sales_query,
        db_user=db_user,
        db_password=db_password,
        db_host=db_host,
        db_name=db_name,
    )

    # Fetch customers data
    df_customers_full = fetch_data_in_batches(
        query=customers_query,
        db_user=db_user,
        db_password=db_password,
        db_host=db_host,
        db_name=db_name,
    )

    customers_from_sales_data_df = fetch_data_in_batches(
        query=customers_from_sales_data_query,
        db_host=db_host,
        db_user=db_user,
        db_password=db_password,
        db_name=db_name,
    )

    # Database connection details
    etani_db_user = os.getenv("ETANI_DB_USER")
    etani_db_password = os.getenv("ETANI_DB_PASSWORD")
    etani_db_server = os.getenv("ETANI_DB_SERVER")
    etani_db_name = os.getenv("ETANI_DB_NAME")

    journals_table_names = ['TAZTECH_CLIENT3_XEROBLUE_Journals',
                            'TAZTECH_CLIENT4_XEROBLUE_Journals',
                            'TAZTECH_CLIENT5_XEROBLUE_Journals',
                            'TAZTECH_CLIENT6_XEROBLUE_Journals',
                            'TAZTECH_CLIENT7_XEROBLUE_Journals',
                            'TAZTECH_CLIENT8_XEROBLUE_Journals',
                            'TAZTECH_CLIENT9_XEROBLUE_Journals'
                            ]
    # Xero journals tables from Etani
    A_Journals = fetch_xero_journals_data_from_etani(db_server=etani_db_server,
                                                     db_name=etani_db_name,
                                                     db_user=etani_db_user,
                                                     db_password=etani_db_password,
                                                     journals_tables_list=journals_table_names,
                                                     query_limit=query_limit)

    # Get active customers
    df_filtered_active_customers = get_filtered_active_customers(customers_from_sales_data_df)

    # Extract follow-up consults
    follow_up_df = get_follow_up_consults(df_sales_full)

    # Extract dental sales after consultation within 14 days
    consults_to_dental_df = get_dental_sales_after_consultation(df_sales_full)

    # Extract lapsed clients
    lapsed_clients_df = get_lapsed_clients(df_sales_full)

    print("Data extraction complete.")
    print("df_filtered_active_customers:", df_filtered_active_customers.shape)
    print("df_sales_full:", df_sales_full.shape)
    print("df_customers_full:", df_customers_full.shape)
    print("follow_up_df:", follow_up_df.shape)
    print("consults_to_dental_df:", consults_to_dental_df.shape)
    print("lapsed_clients_df:", lapsed_clients_df.shape)
    print("A_Journals:", A_Journals.shape)


if __name__ == "__main__":
    main()
