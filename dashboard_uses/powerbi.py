from vetbiz_extractor.utils.common import fetch_data_in_batches
from vetbiz_extractor.core.insights_extractor import (
    get_follow_up_consults,
    get_dental_sales_after_consultation,
    get_lapsed_clients,
)

# Database connection details
db_user = ""
db_password = ""
db_host = ""
db_name = ""

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
LEFT JOIN d_date d ON d.date_tk = s.invoice_date_tk;
"""
df_sales_full = fetch_data_in_batches(
    query=sales_query,
    db_host=db_host,
    db_user=db_user,
    db_password=db_password,
    db_name=db_name,
)

customers_query = """
SELECT cu.*, d.date_field, d.year, d.month, d.month_desc, d.month_short_desc, p.practice_name, c.clinic_name 
FROM d_customer cu
LEFT JOIN d_date d ON d.date_tk = cu.created_tk
LEFT JOIN d_practice p ON p.practice_tk = cu.practice_tk
LEFT JOIN d_clinic c ON c.clinic_tk = cu.clinic_tk
WHERE cu.active = 1 AND cu.created_tk IS NOT NULL;
"""
df_customers_full = fetch_data_in_batches(
    query=customers_query,
    db_host=db_host,
    db_user=db_user,
    db_password=db_password,
    db_name=db_name,
)


customers_from_sales_data_query = """
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
# Get active customers
customers_from_sales_data_df = fetch_data_in_batches(
    query=customers_from_sales_data_query,
    db_host=db_host,
    db_user=db_user,
    db_password=db_password,
    db_name=db_name,
)
df_filtered_active_customers = get_filtered_active_customers(
    customers_from_sales_data_df
)

# Get follow-up consults
follow_up_df = get_follow_up_consults(df_sales_full)

# Get dental sales after consultation within 14 days
consults_dentals_results = get_dental_sales_after_consultation(df_sales_full)

# Get lapsed clients
lapsed_df = get_lapsed_clients(df_sales_full)


# Exposing
df_filtered_active_customers
df_sales_full
df_customers_full
follow_up_df
consults_to_dental_df
lapsed_clients_df
