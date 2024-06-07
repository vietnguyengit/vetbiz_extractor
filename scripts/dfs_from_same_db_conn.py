import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
from datetime import timedelta
import calendar


# Database connection details
db_user = "vetbizread"
db_password = "Vetbiz12345"
db_host = "13.237.124.49"
db_port = 3306
db_name = "vetbizdw"

# Creating the SQLAlchemy engine
connection_string = (
    f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
)
engine = create_engine(connection_string)

sales_query = """
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
    LEFT JOIN d_date d ON d.date_tk = s.invoice_date_tk
"""

"""
SALES FULL
"""
df_sales_full = pd.read_sql_query(sales_query, engine)


"""
CUSTOMERS FULL
"""
customers_query = """
    SELECT cu.*, d.date_field, d.year, d.month, d.month_desc, d.month_short_desc, p.practice_name, c.clinic_name 
    FROM d_customer cu
    LEFT JOIN d_date d ON d.date_tk = cu.created_tk
    LEFT JOIN d_practice p ON p.practice_tk = cu.practice_tk
    LEFT JOIN d_clinic c ON c.clinic_tk = cu.clinic_tk
    WHERE cu.active = 1 AND cu.created_tk IS NOT NULL
"""
df_customers_full = pd.read_sql_query(customers_query, engine)


"""
FOLLOW UP
"""
# Filter the dataframe for consult products only
consults_df = df_sales_full[
    df_sales_full["product_name"].str.contains("consult", case=False, na=False)
]

# Collect sale_ids for follow-up products
sale_ids = []

# Group by customers to eliminate repetitive filtering
grouped = consults_df.groupby("customer_tk")

for customer, group in grouped:
    # Sort by invoice_date
    temp_df = group.sort_values(by="invoice_date").reset_index()

    # Identify sale_ids within 14 days
    for i in range(1, temp_df.shape[0]):
        if (
            temp_df.loc[i, "invoice_date"] - temp_df.loc[i - 1, "invoice_date"]
        ).days <= 14:
            sale_ids.append(temp_df.loc[i, "sale_id"])

# Generate follow-up products dataframe
follow_up_df = consults_df[consults_df["sale_id"].isin(sale_ids)]


"""
CONSULT-DENTAL
"""


# Function to fetch products based on keyword
def get_products_list(products, keyword):
    return [
        product
        for product in products
        if isinstance(product, str) and keyword in product.lower()
    ]


# Get unique product names
products = df_sales_full["product_name"].unique()

# Get dental and consult products
dental_products = get_products_list(products, "dental")
consult_products = get_products_list(products, "consult")

# Filter and process dental products dataframe
dental_df = df_sales_full[
    df_sales_full["product_name"].isin(dental_products)
].reset_index(drop=True)
dental_unique = (
    dental_df[["clinic_tk", "customer_tk", "invoice_date"]]
    .drop_duplicates()
    .reset_index(drop=True)
)
dental_x_series = dental_unique.values

# Filter and process consult products dataframe
consults_df = df_sales_full[
    df_sales_full["product_name"].isin(consult_products)
].reset_index(drop=True)
consults_unique = (
    consults_df[["clinic_tk", "customer_tk", "invoice_date"]]
    .drop_duplicates()
    .reset_index(drop=True)
)
consults_x_series = consults_unique.values

# Find consults followed by dentals within 14 days
consults_dental_results = [
    np.array(dental)
    for consult in consults_x_series
    for dental in dental_x_series
    if consult[1] == dental[1] and (consult[2] + timedelta(days=14)) >= dental[2]
]

# Create final dataframe
consults_dental_results_df = (
    pd.DataFrame(
        consults_dental_results, columns=["clinic_tk", "customer_tk", "invoice_date"]
    )
    .drop_duplicates()
    .reset_index(drop=True)
)


"""
LAPSED CLIENTS
"""

columns = []
for col in df_sales_full.columns:
    columns.append(col.encode("utf-8"))
columns.append("l_period")

months = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]

start_day = 1
all_df_values = []


def end_of_month(year, month):
    return calendar.monthrange(year, month)[1]


counter = 0
for year in range(2018, datetime.now().year):
    for month in months:
        if (
            datetime(datetime.now().year - 1, datetime.now().month + 1, 1)
            >= datetime(year, month, 1)
            > datetime(2018, 7, 1)
        ):
            counter = counter + 1

            if month == 1:
                p1_start = datetime(year - 1, month, start_day)
                p1_end = datetime(year - 1, 12, end_of_month(year - 1, 12))

                p2_start = datetime(year, month, start_day)
                p2_end = datetime(year, 12, end_of_month(year, 12))
            else:
                p1_start = datetime(year - 1, month, start_day)
                p1_end = datetime(year, month - 1, end_of_month(year, month - 1))

                p2_start = datetime(year, month, start_day)
                p2_end = datetime(
                    year + 1, month - 1, end_of_month(year + 1, month - 1)
                )

            p1_df = df_sales_full[
                (df_sales_full.invoice_date >= p1_start)
                & (df_sales_full.invoice_date <= p1_end)
            ]
            p1_customers = set(p1_df["customer_tk"])

            p2_df = df_sales_full[
                (df_sales_full.invoice_date >= p2_start)
                & (df_sales_full.invoice_date <= p2_end)
            ]
            p2_customers = set(p2_df["customer_tk"])

            lapsed_customers = p1_customers - p2_customers

            # create temp df and convert to list then append to the outer list holder
            temp_df = p1_df[p1_df.customer_tk.isin(lapsed_customers)]
            temp_df_values = temp_df.values.tolist()
            for values in temp_df_values:
                val = (
                    str(counter)
                    + ". "
                    + str(p2_start.strftime("%d-%b-%Y"))
                    + " to "
                    + str(p2_end.strftime("%d-%b-%Y"))
                )
                values.append(val)

            all_df_values = all_df_values + temp_df_values

lapsed_df = pd.DataFrame(data=all_df_values, columns=columns)

"""
EXTRACTED DATAFRAMES
"""
print(df_sales_full)
print("------")
print(df_customers_full)
print("------")
print(follow_up_df)
print("------")
print(consults_dental_results_df)
print("------")
print(lapsed_df)
