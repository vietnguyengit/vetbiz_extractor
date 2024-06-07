import calendar
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine

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

# SQL query
query = """
    SELECT s.visit_id, s.sale_id, s.clinic_tk, cus.customer_tk, cus.customer_id, cus.active, 
           pat.patient_id, p.practice_name, c.clinic_name, u.user_name, pr.product_name, 
           s.unit_cost, s.fixed_cost, s.unit_sale, s.fixed_sale, d.date_tk, d.date_field, 
           d.month, d.year, u.clinician 
    FROM f_sales s 
    JOIN d_user u ON u.user_tk = s.user_tk 
    JOIN d_customer cus ON cus.customer_tk = s.customer_tk 
    JOIN d_clinic c ON c.clinic_tk = s.clinic_tk 
    JOIN d_practice p ON p.practice_tk = c.practice_tk 
    JOIN d_product pr ON pr.product_tk = s.product_tk 
    JOIN d_date d ON d.date_tk = s.invoice_date_tk 
    JOIN d_patient pat ON pat.patient_tk = s.patient_tk
"""
source_data_py = pd.read_sql(query, engine)

months = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
active_customers_list = []
for year in range(2018, datetime.now().year):
    for month in months:
        # Get the first and last date of the current month
        start_date = datetime(year, month, 1)
        end_date = datetime(year, month, calendar.monthrange(year, month)[1])

        # Filter data for the current month
        df_temp = source_data_py[
            (source_data_py["date_field"] >= start_date)
            & (source_data_py["date_field"] <= end_date)
        ]

        # Get unique customers for the current month
        unique_customers = set(df_temp.customer_tk.unique())

        # Calculate date 18 months prior to the start of the current month
        before_date = start_date - relativedelta(months=18)

        # Filter data for the last 18 months
        df_last_18m = source_data_py[
            (source_data_py["date_field"] >= before_date)
            & (source_data_py["date_field"] < start_date)
        ]

        # Get unique customers for the last 18 months
        unique_last_18m = set(df_last_18m.customer_tk.unique())

        # Find active customers in the intersection and filter the temporary DataFrame
        active_customers = unique_customers.intersection(unique_last_18m)
        df_active_clients_temp = df_temp[df_temp["customer_tk"].isin(active_customers)]

        # Convert DataFrame to list and append to active customers list
        active_temp_list = df_active_clients_temp.values.tolist()
        active_customers_list.append(active_temp_list)


# Column titles constant declaration
COLUMN_TITLES = [
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

# Create list of DataFrames for active customers and concatenate them into a single DataFrame
dfs = [pd.DataFrame(data, columns=COLUMN_TITLES) for data in active_customers_list]
df_filtered_active_customers = pd.concat(dfs, ignore_index=True)
