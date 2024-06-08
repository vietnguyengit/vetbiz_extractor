import pandas as pd
from datetime import datetime, timedelta
from vetbiz_extractor.utils.common import fetch_data_in_batches, end_of_month


class ExtractInsights:
    def __init__(self, db_user, db_password, db_host, db_port, db_name, query_limit):
        self.db_user = db_user
        self.db_password = db_password
        self.db_host = db_host
        self.db_port = db_port
        self.db_name = db_name
        self.query_limit = query_limit

    def fetch_data(self, query):
        return fetch_data_in_batches(
            query,
            self.db_user,
            self.db_password,
            self.db_host,
            self.db_port,
            self.db_name,
        )

    @staticmethod
    def get_products_list(products, keyword):
        return [
            product
            for product in products
            if isinstance(product, str) and keyword in product.lower()
        ]

    def extract_follow_up_sales_after_consultation(self, df_sales_full):
        all_products = df_sales_full["product_name"].unique()
        consult_products = self.get_products_list(all_products, "consult")

        consults_df = df_sales_full[df_sales_full.product_name.isin(consult_products)]
        # this workflow involves comparing days difference (e.g 14 days threshold),
        # it's important to sort with invoice_date first
        sorted_consults_df = consults_df.sort_values(by=["invoice_date"])
        # Follow-up counts for each customer.
        # A single customer may be linked to multiple consults, so group by customer as well.
        consults_grouped_by_customer = sorted_consults_df.groupby("customer_tk")

        def get_follow_up_sales_from_consults(consults_grouped_df, days_threshold=14):
            """
            Extract follow-up sale IDs from consults (grouped by customer)
            for follow-up sales based on invoice dates within a given threshold.
            :param consults_grouped_df:
            :param days_threshold:
            :return:
            """
            sale_ids = [
                group["sale_id"].iloc[i]
                for _, group in consults_grouped_df
                for i in range(1, len(group))
                if (
                    group["invoice_date"].iloc[i] - group["invoice_date"].iloc[i - 1]
                ).days
                <= days_threshold
            ]
            return sale_ids

        follow_up_sale_ids = get_follow_up_sales_from_consults(
            consults_grouped_by_customer
        )
        return consults_df[consults_df["sale_id"].isin(follow_up_sale_ids)].reset_index(
            drop=True
        )

    def extract_dental_sales_after_consultation(self, df_sales_full):
        all_products = df_sales_full["product_name"].unique()
        dental_products = self.get_products_list(all_products, "dental")
        consult_products = self.get_products_list(all_products, "consult")

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
            consult_products_sale_records, dental_products_sale_records, days=14
        ):
            """
            find dental sales that occurred within 14 days (or days variable) after a consultation
            :param consult_products_sale_records:
            :param dental_products_sale_records:
            :param days:
            :return:
            """
            results = []
            for consult_products_sale_record in consult_products_sale_records:
                for dental_products_sale_record in dental_products_sale_records:
                    if (
                        consult_products_sale_record[1]
                        == dental_products_sale_record[1]
                        and consult_products_sale_record[2] + timedelta(days=days)
                        <= dental_products_sale_record[2]
                    ):
                        results.append(dental_products_sale_record)
            return (
                pd.DataFrame(
                    results, columns=["clinic_tk", "customer_tk", "invoice_date"]
                )
                .drop_duplicates()
                .reset_index(drop=True)
            )

        # Filter dental products
        unique_sales_by_dental_products = filter_sales_data(
            df_sales_full, dental_products
        )
        # Filter consult products
        unique_sales_by_consult_products = filter_sales_data(
            df_sales_full, consult_products
        )

        return find_dental_sales_within_days(
            unique_sales_by_consult_products, unique_sales_by_dental_products
        )

    def extract_lapsed_clients(self, df_sales_full):
        columns = list(df_sales_full.columns) + ["l_period"]
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

                    p1_df = df_sales_full[
                        (df_sales_full.invoice_date >= p1_start)
                        & (df_sales_full.invoice_date <= p1_end)
                    ]
                    p2_df = df_sales_full[
                        (df_sales_full.invoice_date >= p2_start)
                        & (df_sales_full.invoice_date <= p2_end)
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
