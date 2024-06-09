import pandas as pd
import calendar
from datetime import datetime, timedelta
from vetbiz_extractor.utils.common import (
    end_of_month,
    get_products_list,
    get_date_range_for_month,
    filter_data_for_date_range,
)
from dateutil.relativedelta import relativedelta


def get_follow_up_consults(sales_data, days_threshold=14):
    all_products = sales_data["product_name"].unique()
    consult_products = get_products_list(all_products, "consult")

    consults_df = sales_data[sales_data.product_name.isin(consult_products)]
    # this workflow involves comparing days difference (e.g 14 days threshold),
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


def get_dental_sales_after_consultation(sales_data, days_threshold=14):
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


def get_lapsed_clients(sales_data):
    columns = list(sales_data.columns) + ["l_period"]
    current_year, current_month = datetime.now().year, datetime.now().month
    all_df_values = []
    counter = 0

    def get_lapsed_customers(p1_df, p2_df):
        return set(p1_df["customer_tk"]) - set(p2_df["customer_tk"])

    for year in range(2018, current_year + 1):
        for month in range(1, 13):
            if (
                datetime(current_year - 1, current_month + 1, 1)
                >= datetime(year, month, 1)
                > datetime(2018, 7, 1)
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


def get_filtered_active_customers(customers_from_sales_data_df, months_threshold=18):
    """
    get filtered active customers during the last <months_threshold> (default is 18 months)
    :param customers_from_sales_data_df:
    :param months_threshold:
    :return:
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

    def get_customers_from_period(
        customers_from_sales_data_df, start_date, months_threshold
    ):
        before_date = start_date - relativedelta(months=months_threshold)
        period_data_df = filter_data_for_date_range(
            customers_from_sales_data_df,
            before_date,
            start_date - relativedelta(days=1),
        )
        return get_unique_customers(period_data_df)

    for year in range(2018, current_year + 1):
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
