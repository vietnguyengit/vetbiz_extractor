import pymssql
import pandas as pd

# Database connection details
etani_db_user = ""
etani_db_password = ""
etani_db_server = ""
etani_db_name = "TAZTECH_d434a453-c859-41d1-93b7-f367a1572792"

# Creating the SQLAlchemy engine
conn = pymssql.connect(
    server=etani_db_server,
    user=etani_db_user,
    password=etani_db_password,
    database=etani_db_name,
)

taztech_client3_table = pd.read_sql_query(
    "SELECT * FROM TAZTECH_CLIENT3_XEROBLUE_Journals", conn
)
taztech_client4_table = pd.read_sql_query(
    "SELECT * FROM TAZTECH_CLIENT4_XEROBLUE_Journals", conn
)
taztech_client5_table = pd.read_sql_query(
    "SELECT * FROM TAZTECH_CLIENT5_XEROBLUE_Journals", conn
)
taztech_client6_table = pd.read_sql_query(
    "SELECT * FROM TAZTECH_CLIENT6_XEROBLUE_Journals", conn
)
taztech_client7_table = pd.read_sql_query(
    "SELECT * FROM TAZTECH_CLIENT7_XEROBLUE_Journals", conn
)
taztech_client8_table = pd.read_sql_query(
    "SELECT * FROM TAZTECH_CLIENT8_XEROBLUE_Journals", conn
)
taztech_client9_table = pd.read_sql_query(
    "SELECT * FROM TAZTECH_CLIENT9_XEROBLUE_Journals", conn
)

frames = [
    taztech_client3_table,
    taztech_client4_table,
    taztech_client5_table,
    taztech_client6_table,
    taztech_client7_table,
    taztech_client8_table,
    taztech_client9_table,
]

A_Journals = pd.concat(frames)
