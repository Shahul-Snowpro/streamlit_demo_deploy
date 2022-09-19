import streamlit as st
import snowflake.connector as sc
import pandas as p

@st.experimental_singleton(suppress_st_warning=True)

def init_connection():
    return sc.connect(**st.secrets["vbi_toml"])

connection = init_connection()

@st.experimental_memo()
def run_query(query):
    with connection.cursor() as cur:
        cur.execute(query)
        return cur.fetchall()

file_loader,buff1 = st.columns(2)

with file_loader:
    file_upload = st.file_uploader("Choose a file")
with buff1:
    pass


username, password = st.columns(2)

with username:
    un = st.text_input("User Name",placeholder="Please enter the username!!!")
with password:
    pw = st.text_input("Password",type="password") 

database, schema, tablename, uni_cols = st.columns(4)
if un != '' and pw != '':
    with database:
        database= run_query(f'''SELECT DATABASE_NAME FROM information_schema.databases ''')
        database = p.DataFrame(database,columns=["Database Name"])
        selectbox_database = st.selectbox("Database",database)
    with schema:
        schema = run_query(f'''SELECT SCHEMA_NAME FROM information_schema.schemata WHERE CATALOG_NAME = '{selectbox_database}' ''')
        schema = p.DataFrame(schema,columns=["Schema Name"])
        selectbox_schema= st.selectbox("Schema",schema)


    checkbox = st.checkbox("Add new table")
    if checkbox == True:
        with tablename:
            table_name = st.text_input("Table Name",placeholder="Please type the table name!!!")
    else:
        with tablename:
            Sel_table_name = run_query(f'''SELECT TABLE_NAME FROM information_schema.tables WHERE table_catalog = '{selectbox_database}' AND TABLE_SCHEMA = '{selectbox_schema}' ''')
            Sel_table_name = p.DataFrame(Sel_table_name,columns=["Table Name"])
            table_name= st.selectbox("Tables",Sel_table_name)
            if table_name:
                table_name_temp = table_name+'_Temp'

