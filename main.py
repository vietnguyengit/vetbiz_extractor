from utils.utils import measure_execution_time, validate_queries, validate_env_vars
from core.extract_insights import ExtractInsights
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
    db_port = os.getenv("DB_PORT", 3306)
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

    # Add query limit if specified
    if query_limit is not None:
        sales_query += f" LIMIT {query_limit}"
        customers_query += f" LIMIT {query_limit}"

    insights_extractor = ExtractInsights(
        db_user, db_password, db_host, int(db_port), db_name, query_limit
    )

    # Fetch sales data
    df_sales_full = insights_extractor.fetch_data(sales_query)

    # Fetch customers data
    df_customers_full = insights_extractor.fetch_data(customers_query)

    # Extract follow-up consults
    follow_up_df = insights_extractor.extract_follow_up_consults(df_sales_full)

    # Extract consult-dental data
    consults_dental_results_df = insights_extractor.extract_consults_dental_data(
        df_sales_full
    )

    # Extract lapsed clients
    lapsed_df = insights_extractor.extract_lapsed_clients(df_sales_full)

    print("Data extraction complete.")
    print("df_sales_full:", df_sales_full.shape)
    print("df_customers_full:", df_customers_full.shape)
    print("follow_up_df:", follow_up_df.shape)
    print("consults_dental_results_df:", consults_dental_results_df.shape)
    print("lapsed_df:", lapsed_df.shape)


if __name__ == "__main__":
    main()
