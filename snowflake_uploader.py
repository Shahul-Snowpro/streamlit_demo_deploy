import streamlit as st
import pandas as p
import snowflake.connector as sc
sc.paramstyle='qmark'
from datetime import datetime as d
import getpass as gp

st.set_page_config(
    layout='wide'
)


# st.image('epsonlogo.jpg')

@st.experimental_singleton()
def init_connection():
    return sc.connect(**st.secrets["vbi"])

connection= init_connection()

@st.experimental_memo(ttl=60)
def run_query(query):
    with connection.cursor() as cur:
        cur.execute(query)
        return cur.fetchall()

file_loader,extension,headers,ch_sh = st.columns(4)

with file_loader:
    file_upload = st.file_uploader("Choose a file",["csv","xlsx"])
with extension:
    ext_radio = st.radio("Select extension",["csv","xlsx"],horizontal=True)


if file_upload is not None:
    database, schema, tablename, uni_cols = st.columns(4)

    with headers:
        head = st.radio("Do you want load with headers?",['Yes','No'],horizontal=True)

    with database:
        database= run_query(f'''SELECT DATABASE_NAME FROM information_schema.databases ''')
        database = p.DataFrame(database,columns=["Database Name"]).sort_values(by='Database Name',ascending=True)
        selectbox_database = st.selectbox("Database",database)
    with schema:
        schema = run_query(f'''SELECT SCHEMA_NAME FROM information_schema.schemata WHERE CATALOG_NAME = '{selectbox_database}' ''')
        schema = p.DataFrame(schema,columns=["Schema Name"]).sort_values(by='Schema Name',ascending=True)
        selectbox_schema= st.selectbox("Schema",schema)

    radio_button, is_trunc, Recreate = st.columns(3)

    if head == 'Yes':
        with radio_button:
            radio_bt = st.radio("Choose type of action",["Insert","Update","New Table"],horizontal=True)
    else:
        with radio_button:
            radio_bt = st.radio("Choose type of action",["Insert","Update"],horizontal=True)
    if radio_bt == "Insert":
        with is_trunc:
            trunc = st.radio("Do you want to truncate and load table?",["Yes","No"],horizontal=True)
        with Recreate:
                recr = st.radio("Want to recreate the table and load?",["Yes","No"],horizontal=True,help='djashd')

    if radio_bt == "New Table":
        with tablename:
            table_name = st.text_input("Table Name",placeholder="Please type the table name!!!")
    else:
        with tablename:
            Sel_table_name = run_query(f'''SELECT TABLE_NAME FROM information_schema.tables WHERE table_catalog = '{selectbox_database}' AND TABLE_SCHEMA = '{selectbox_schema}' ''')
            Sel_table_name = p.DataFrame(Sel_table_name,columns=["Table Name"]).sort_values(by='Table Name',ascending=True)
            table_name= st.selectbox("Tables",Sel_table_name)
            if table_name:
                if radio_bt == 'Update':
                    table_name_temp = table_name+'_Temp'


    
    if ext_radio == "csv": 
        if head == 'Yes':
            csv_reader = p.read_csv(file_upload)
        else:
            csv_reader = p.read_csv(file_upload,header=None)
    if ext_radio == "xlsx":
        if head == 'Yes':
            xl_reader = p.read_excel(file_upload,sheet_name=None)
            sheets = xl_reader.keys()
        else:
            xl_reader = p.read_excel(file_upload,sheet_name=None,header=None)
            sheets = xl_reader.keys()
        with ch_sh:
            if len(sheets) > 1:
                choose_sheet = st.selectbox("Select sheets",sheets)
                csv_reader = xl_reader[choose_sheet]
            else:    
                csv_reader = p.read_excel(file_upload)
    
    csv_reader = csv_reader.dropna(how='all',axis='columns')
    csv_reader = csv_reader.fillna(0) 
    
    if (radio_bt == "New Table" or radio_bt == "Insert") and head == "Yes":
        csv_reader["filename"] = file_upload.name
        csv_reader["Load_time"] = d.now().strftime("%m/%d/%Y %H:%M:%S")
        csv_reader["loaded_by"] = gp.getuser()
        csv_reader["Update_time"] = 'Null'
        csv_reader["Updated_by"] = 'Null'
    if radio_bt == "Update" and head == "Yes":
        csv_reader["filename"] = file_upload.name
        csv_reader["Update_time"] = d.now().strftime("%m/%d/%Y %H:%M:%S")
        csv_reader["Updated_by"] = gp.getuser()

    header_datatype = csv_reader.dtypes.index

    sel = ''
    for col in header_datatype:
        sel += f''' "{col}" ''' +','
    sel = sel.rstrip(",")

    def dtype_mapping():
        if ext_radio == "csv":
            return {'object' : 'VARCHAR',
            'int64' : 'NUMBER',
            'uint64': 'NUMBER',
            'float64' : 'FLOAT',
            'datetime64' : 'TIMESTAMP_NTZ',
            'bool' : 'BOOLEAN',
            'category' : 'TEXT',
            'timedelta[ns]' : 'TIMESTAMP_NTZ'}
        elif ext_radio == "xlsx":
            return {'object' : 'VARCHAR',
            'int64' : 'NUMBER',
            'uint64': 'NUMBER',
            'float64' : 'FLOAT',
            'datetime64[ns]' : 'TIMESTAMP_NTZ',
            'bool' : 'BOOLEAN',
            'category' : 'TEXT',
            'timedelta[ns]' : 'TIMESTAMP_NTZ'}

    dt_map = dtype_mapping()


    hdrs_list = [(hdr, str(csv_reader[hdr].dtype)) for hdr in header_datatype]

    header_datatype_df = p.DataFrame(header_datatype,columns=["Column Name"])
    
    if radio_bt == "Update":
        with uni_cols:
            unique_col = st.multiselect("Please select the unique columns",header_datatype)
        if unique_col:
            if len(unique_col) == 1:
                unique_col_tuple = ''.join(str(unique_col[0]))
                condition_columns = f''' t."{unique_col_tuple}" =  c."{unique_col_tuple}" '''
                header_datatype_df_con = header_datatype_df.loc[header_datatype_df["Column Name"] != f"{unique_col_tuple}"]
            else:
                unique_cols = unique_col
                condition_columns = ''
                for u in unique_cols:
                    condition_columns += f'''and t."{u}" =  c."{u}"'''                
                condition_columns = condition_columns.lstrip("and")
                for l in unique_cols:
                    if l == unique_cols[0]:
                        header_datatype_df_col = header_datatype_df.loc[header_datatype_df["Column Name"] != f"{l}"]
                    header_datatype_df_con = header_datatype_df_col.loc[header_datatype_df_col["Column Name"] != f"{l}"]


    if st.button("Submit"):
        if radio_bt == "New Table":            
            sql = f'''CREATE OR REPLACE TABLE {selectbox_database}.{selectbox_schema}.{table_name} ('''
            for hl in hdrs_list:
                sql += ' ,"{0}" {1}'.format(hl[0], dt_map[hl[1]])
            sql = sql.replace(",","",1)+');'
   
            cret_tab = run_query(sql)
            
            dataingest = [tuple(i) for i in csv_reader.values]
            dataingest_tuple = tuple(dataingest)

            qmark = ''
            for d in range (csv_reader.shape[1]):
                qmark += ', ?'
            qmark = qmark.lstrip(',')

            
            sqltext = f""" INSERT INTO {selectbox_database}.{selectbox_schema}.{table_name} ({sel}) VALUES ({qmark}) """
            connection.cursor().executemany(sqltext,dataingest_tuple)  
           

            st.success(f"Sucessfully created new table: {table_name}")          
            
        elif radio_bt == "Update":
            sql = f'''CREATE OR REPLACE Temporary TABLE {selectbox_database}.{selectbox_schema}.{table_name_temp} ('''
            
            for hl in hdrs_list:
                sql += ' \n ,"{0}" {1}'.format(hl[0], dt_map[hl[1]])
            sql = sql.replace(",","",1)+')'

            connection.cursor().execute(sql)
            
            dataingest = [tuple(i) for i in csv_reader.values]
            dataingest_tuple = tuple(dataingest)

            qmark = ''
            for d in range (csv_reader.shape[1]):
                qmark += ', ?'
            qmark = qmark.lstrip(',')

            sqltext = f""" INSERT INTO {selectbox_database}.{selectbox_schema}.{table_name_temp} ({sel}) VALUES ({qmark}) """
            connection.cursor().executemany(sqltext,dataingest_tuple)

            df_col = p.DataFrame(header_datatype_df_con["Column Name"],columns=["Column Name"] )
            df_col.reset_index(inplace=True,drop=True)
            
            set_col = ''
            
            csv_dt = csv_reader.dtypes.tolist()
            
            for i , row in df_col.iterrows():
                if csv_dt[i] == 'timedelta[ns]':
                    set_col += "t."+f'"{row["Column Name"]}"'+"= date(c."+f'"{row["Column Name"]}"'+")"+","
                set_col += "t."+f'"{row["Column Name"]}"'+"= c."+f'"{row["Column Name"]}"'+","
            set_col = set_col.rstrip(",")
            
            update_sql = f'''update {selectbox_database}.{selectbox_schema}.{table_name}  as t\n set\n {set_col}
                                    from {selectbox_database}.{selectbox_schema}.{table_name_temp} c\n where {condition_columns}'''
            run_query(update_sql)

            st.success(f"Successfully updated values in the table: {table_name}")

        elif radio_bt == "Insert":
            if recr == "Yes":
                sql = f'''CREATE OR REPLACE TABLE {selectbox_database}.{selectbox_schema}.{table_name} ('''
                for hl in hdrs_list:
                    sql += ' ,"{0}" {1}'.format(hl[0], dt_map[hl[1]])
                sql = sql.replace(",","",1)+');'
                cret_tab = run_query(sql)

            dataingest = [tuple(i) for i in csv_reader.values]
            dataingest_tuple = tuple(dataingest)

            qmark = ''
            for d in range (csv_reader.shape[1]):
                qmark += ', ?'
            qmark = qmark.lstrip(',')

            if trunc == "Yes":
                trunc_sql = f'TRUNCATE TABLE {selectbox_database}.{selectbox_schema}.{table_name};'
                connection.cursor().execute(trunc_sql)

            if head == 'No':
                sqltext = f""" INSERT INTO {selectbox_database}.{selectbox_schema}.{table_name} VALUES ({qmark}) """
                connection.cursor().executemany(sqltext,dataingest_tuple)
                rowcount = connection.cursor().execute('select $1 from table(result_scan(last_query_id()))')
                rowcount = p.DataFrame(rowcount,columns=["Total count inserted"])
                rowcnt = rowcount["Total count inserted"]
                rowcnt = p.Series(rowcnt)
                rc = rowcnt[0]
                st.write(rc)
            else:
                sqltext = f""" INSERT INTO {selectbox_database}.{selectbox_schema}.{table_name} ({sel}) VALUES ({qmark}) """
                connection.cursor().executemany(sqltext,dataingest_tuple)
                rowcount = connection.cursor().execute('select $1 from table(result_scan(last_query_id()))')
                rowcount = p.DataFrame(rowcount,columns=['Total count inserted'])
                rowcnt = rowcount["Total count inserted"]
                rowcnt = p.Series(rowcnt)
                rc = rowcnt[0]
                st.write(rc)
            if trunc == "Yes":
                st.success(f""" Successfully truncated and loaded data into {table_name}\n
                                Total Number of records inserted: {rc}""")
                lastinserted = run_query(f"""SELECT * FROM {selectbox_database}.{selectbox_schema}.{table_name} ORDER BY "Load_time" LIMIT 1000 """)
                lastinserted = p.DataFrame(lastinserted,columns=[sel])
                st.write(lastinserted)
            else:
                st.success(f""" Successfully truncated and loaded data into {table_name}\n
                                Total Number of records inserted: {rc}""")
                lastinserted = run_query(f"""SELECT * FROM {selectbox_database}.{selectbox_schema}.{table_name} ORDER BY "Load_time" LIMIT 1000 """)
                lastinserted = p.DataFrame(lastinserted,columns=sel)
                st.write(lastinserted) 
