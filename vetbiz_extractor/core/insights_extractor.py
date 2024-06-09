import pandas as pd
from datetime import datetime, timedelta
from vetbiz_extractor.utils.common import (
    end_of_month,
    get_products_list,
    get_date_range_for_month,
    filter_data_for_date_range,
)
from dateutil.relativedelta import relativedelta


def get_follow_up_consults(
    sales_data: pd.DataFrame, days_threshold: int = 14
) -> pd.DataFrame:
    """
    Filter the sales data to retrieve follow-up consults within a specified days threshold.

    :param sales_data: DataFrame containing sales data.
    :param days_threshold: Number of days to define the follow-up threshold (default is 14 days).
    :return: DataFrame filtered for follow-up consults within the specified days threshold.
    """

    all_products = sales_data["product_name"].unique()
    consult_products = get_products_list(all_products, "consult")

    consults_df = sales_data[sales_data.product_name.isin(consult_products)]
    # this workflow involves comparing days difference
    # it's important to sort with invoice_date first
    sorted_consults_df = consults_df.sort_values(by=["invoice_date"])
    # Follow-up counts for each customer.
    # A single customer may be linked to multiple consults, so group by customer as well.
    consults_grouped_by_customer = sorted_consults_df.groupby("customer_tk")

    def get_follow_up_within_days(consults_grouped_df, period):
        """
        Extract follow-up sale IDs from consults (grouped by customer)
        for follow-up sales based on invoice dates within a given period.
        :param consults_grouped_df:
        :param period:
        :return:
        """
        sale_ids = [
            group["sale_id"].iloc[i]
            for _, group in consults_grouped_df
            for i in range(1, len(group))
            if (group["invoice_date"].iloc[i] - group["invoice_date"].iloc[i - 1]).days
            <= period
        ]
        return sale_ids

    follow_up_sale_ids = get_follow_up_within_days(
        consults_grouped_by_customer, days_threshold
    )
    return consults_df[consults_df["sale_id"].isin(follow_up_sale_ids)].reset_index(
        drop=True
    )


def get_dental_sales_after_consultation(
    sales_data: pd.DataFrame, days_threshold: int = 14
) -> pd.DataFrame:
    """
    Filter the sales data to retrieve dental sales made after consultations within a specified days threshold.

    :param sales_data: DataFrame containing sales data.
    :param days_threshold: Number of days to define the threshold for sales after consultation (default is 14 days).
    :return: DataFrame filtered for dental sales made after consultations within the specified days threshold.
    """

    all_products = sales_data["product_name"].unique()
    dental_products = get_products_list(all_products, "dental")
    consult_products = get_products_list(all_products, "consult")

    def filter_sales_data(df, products):
        """
        Filters and returns unique sales records for a given list of products
        :param df:
        :param products:
        :return:
        """
        filtered_df = df[df.product_name.isin(products)]
        unique_sales = (
            filtered_df[["clinic_tk", "customer_tk", "invoice_date"]]
            .drop_duplicates()
            .reset_index(drop=True)
        )
        return unique_sales.values

    def find_dental_sales_within_days(
        consult_products_sale_records, dental_products_sale_records, period
    ):
        """
        find dental sales that occurred within 14 days (or period variable) after a consultation
        :param consult_products_sale_records:
        :param dental_products_sale_records:
        :param period:
        :return:
        """
        results = []
        for consult_products_sale_record in consult_products_sale_records:
            for dental_products_sale_record in dental_products_sale_records:
                if consult_products_sale_record[1] == dental_products_sale_record[
                    1
                ] and consult_products_sale_record[2] <= dental_products_sale_record[
                    2
                ] <= consult_products_sale_record[
                    2
                ] + timedelta(
                    days=period
                ):
                    results.append(dental_products_sale_record)
        return (
            pd.DataFrame(results, columns=["clinic_tk", "customer_tk", "invoice_date"])
            .drop_duplicates()
            .reset_index(drop=True)
        )

    # Filter dental products
    unique_dental_products_sales = filter_sales_data(sales_data, dental_products)
    # Filter consult products
    unique_consult_products_sales = filter_sales_data(sales_data, consult_products)

    return find_dental_sales_within_days(
        unique_consult_products_sales,
        unique_dental_products_sales,
        days_threshold,
    )


def get_lapsed_clients(
    sales_data: pd.DataFrame, start_year: int = 2018
) -> pd.DataFrame:
    """
    Identify and filter lapsed clients from the sales data.

    A lapsed client is defined as a client who has not made any purchases since a specified start year.

    :param sales_data: Pandas DataFrame containing sales data.
                       It must include a 'last_purchase_date' column with dates of the last purchase.
    :param start_year: integer representing the start year from which to measure inactivity.
                       Defaults to 2018.
    :return: A pandas DataFrame filtered to include only the lapsed clients who have not made a purchase since the start year.
    """

    columns = list(sales_data.columns) + ["l_period"]
    current_year, current_month = datetime.now().year, datetime.now().month
    all_df_values = []
    counter = 0

    def get_lapsed_customers(df1, df2):
        return set(df1["customer_tk"]) - set(df2["customer_tk"])

    for year in range(start_year, current_year + 1):
        for month in range(1, 13):
            if (
                datetime(current_year - 1, current_month + 1, 1)
                >= datetime(year, month, 1)
                > datetime(start_year, 7, 1)
            ):
                counter += 1
                p1_start, p1_end = (
                    (datetime(year - 1, month, 1), datetime(year - 1, 12, 31))
                    if month == 1
                    else (
                        datetime(year - 1, month, 1),
                        datetime(year, month - 1, end_of_month(year, month - 1)),
                    )
                )
                p2_start, p2_end = (
                    (datetime(year, month, 1), datetime(year, 12, 31))
                    if month == 1
                    else (
                        datetime(year, month, 1),
                        datetime(
                            year + 1, month - 1, end_of_month(year + 1, month - 1)
                        ),
                    )
                )

                p1_df = sales_data[
                    (sales_data.invoice_date >= p1_start)
                    & (sales_data.invoice_date <= p1_end)
                ]
                p2_df = sales_data[
                    (sales_data.invoice_date >= p2_start)
                    & (sales_data.invoice_date <= p2_end)
                ]

                temp_df = p1_df[
                    p1_df.customer_tk.isin(get_lapsed_customers(p1_df, p2_df))
                ]
                temp_df_values = temp_df.values.tolist()
                for values in temp_df_values:
                    l_period = f"{counter}. {p2_start.strftime('%d-%b-%Y')} to {p2_end.strftime('%d-%b-%Y')}"
                    values.append(l_period)
                all_df_values = all_df_values + temp_df_values

    return pd.DataFrame(data=all_df_values, columns=columns)


def get_filtered_active_customers(
    customers_from_sales_data_df: pd.DataFrame,
    start_year: int = 2020,
    months_threshold: int = 18,
) -> pd.DataFrame:
    """
    Filter the customers who have been active within a specified number of months since a given start year.

    This function filters out customers from the provided sales data DataFrame
    who have made purchases within the specified months threshold since the start year.

    :param customers_from_sales_data_df: Pandas DataFrame containing customer sales data.
                                         It must include a 'last_purchase_date' column.
    :param start_year: integer representing the start year from which to measure activity.
                       Defaults to 2020.
    :param months_threshold: Integer representing the number of months within which a customer
                             must have made a purchase to be considered active. Defaults to 18 months.
    :return: A pandas DataFrame filtered to include only the active customers.
    """

    filtered_active_customers_list = []
    active_customers_df_columns = [
        "visit_id",
        "sale_id",
        "clinic_tk",
        "customer_tk",
        "customer_id",
        "active",
        "patient_id",
        "practice_name",
        "clinic_name",
        "user_name",
        "product_name",
        "unit_cost",
        "fixed_cost",
        "unit_sale",
        "fixed_sale",
        "date_tk",
        "date_field",
        "month",
        "year",
        "clinician",
    ]
    current_year = datetime.now().year

    def get_unique_customers(df):
        """Returns a set of unique customers from the DataFrame."""
        return set(df.customer_tk.unique())

    def get_customers_from_period(input_df, start, threshold):
        before_date = start - relativedelta(months=threshold)
        period_data_df = filter_data_for_date_range(
            input_df,
            before_date,
            start - relativedelta(days=1),
        )
        return get_unique_customers(period_data_df)

    for year in range(start_year, current_year + 1):
        for month in range(1, 13):
            start_date, end_date = get_date_range_for_month(year, month)
            current_month_data_df = filter_data_for_date_range(
                customers_from_sales_data_df, start_date, end_date
            )

            all_customers_current_month = get_unique_customers(current_month_data_df)
            all_customers_from_period = get_customers_from_period(
                customers_from_sales_data_df, start_date, months_threshold
            )

            # Find active customers in the intersection
            active_customers_current_month = all_customers_current_month.intersection(
                all_customers_from_period
            )
            active_customers_current_month_df = current_month_data_df[
                current_month_data_df["customer_tk"].isin(
                    active_customers_current_month
                )
            ]

            # Convert DataFrame to list and append to active customers list
            filtered_active_customers_list.extend(
                active_customers_current_month_df.values.tolist()
            )

    # Create a DataFrame for all filtered active customers
    return pd.DataFrame(
        filtered_active_customers_list, columns=active_customers_df_columns
    ).reset_index(drop=True)
