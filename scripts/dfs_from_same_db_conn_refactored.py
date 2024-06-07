import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta
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


# Function to fetch data from database
def fetch_data(query, engine):
    return pd.read_sql_query(query, engine)


# Fetch sales data
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
df_sales_full = fetch_data(sales_query, engine)

# Fetch customers data
customers_query = """
    SELECT cu.*, d.date_field, d.year, d.month, d.month_desc, d.month_short_desc, p.practice_name, c.clinic_name 
    FROM d_customer cu
    LEFT JOIN d_date d ON d.date_tk = cu.created_tk
    LEFT JOIN d_practice p ON p.practice_tk = cu.practice_tk
    LEFT JOIN d_clinic c ON c.clinic_tk = cu.clinic_tk
    WHERE cu.active = 1 AND cu.created_tk IS NOT NULL
"""
df_customers_full = fetch_data(customers_query, engine)

# Extract follow-up consults
consults_df = df_sales_full[
    df_sales_full["product_name"].str.contains("consult", case=False, na=False)
]
consults_df = consults_df.sort_values(by=["customer_tk", "invoice_date"])

follow_up_groups = consults_df.groupby("customer_tk")
sale_ids = [
    group["sale_id"].iloc[i]
    for customer, group in follow_up_groups
    for i in range(1, len(group))
    if (group["invoice_date"].iloc[i] - group["invoice_date"].iloc[i - 1]).days <= 14
]
follow_up_df = consults_df[consults_df["sale_id"].isin(sale_ids)]


# Extract consult-dental data
def get_products_list(products, keyword):
    return [
        product
        for product in products
        if isinstance(product, str) and keyword in product.lower()
    ]


products = df_sales_full["product_name"].unique()
dental_products = get_products_list(products, "dental")
consult_products = get_products_list(products, "consult")

dental_df = df_sales_full[df_sales_full["product_name"].isin(dental_products)]
dental_unique = dental_df[
    ["clinic_tk", "customer_tk", "invoice_date"]
].drop_duplicates()

consults_df = df_sales_full[df_sales_full["product_name"].isin(consult_products)]
consults_unique = consults_df[
    ["clinic_tk", "customer_tk", "invoice_date"]
].drop_duplicates()

consults_dental_results = [
    dental
    for consult in consults_unique.itertuples(index=False)
    for dental in dental_unique.itertuples(index=False)
    if consult.customer_tk == dental.customer_tk
    and (consult.invoice_date + timedelta(days=14)) >= dental.invoice_date
]

consults_dental_results_df = pd.DataFrame(
    consults_dental_results, columns=["clinic_tk", "customer_tk", "invoice_date"]
).drop_duplicates()


# Extract lapsed clients
def end_of_month(year, month):
    return calendar.monthrange(year, month)[1]


columns = list(df_sales_full.columns) + ["l_period"]
current_year, current_month = datetime.now().year, datetime.now().month
all_df_values = []
counter = 0


def get_lapsed_customers(p1_df, p2_df):
    return set(p1_df["customer_tk"]) - set(p2_df["customer_tk"])


for year in range(2018, current_year):
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
                    datetime(year + 1, month - 1, end_of_month(year + 1, month - 1)),
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

            temp_df = p1_df[p1_df.customer_tk.isin(get_lapsed_customers(p1_df, p2_df))]

            l_period = f"{counter}. {p2_start.strftime('%d-%b-%Y')} to {p2_end.strftime('%d-%b-%Y')}"
            temp_df["l_period"] = l_period

            all_df_values.append(temp_df)

lapsed_df = pd.concat(all_df_values, ignore_index=True)

# Display extracted dataframes
print(df_sales_full)
print("------")
print(df_customers_full)
print("------")
print(follow_up_df)
print("------")
print(consults_dental_results_df)
print("------")
print(lapsed_df)
