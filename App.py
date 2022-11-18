# streamlit_app.py

import streamlit as st 
from google.oauth2 import service_account
from google.cloud import bigquery
import pandas as pd
import pandas_gbq
import datetime
from datetime import datetime

# Create API client.
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"])
client = bigquery.Client(credentials=credentials)

# Perform query.
# Uses st.experimental_memo to only rerun when the query changes or after 10 min.
@st.experimental_memo(ttl=600)
def run_query(query):
    query_job = client.query(query)
    rows_raw = query_job.result()
    # Convert to list of dicts. Required for st.experimental_memo to hash the return value.
    rows = [dict(row) for row in rows_raw]
    return rows

rows = run_query("SELECT * FROM `Ma.Sensor` where Station = 18")
files_id = pd.read_csv('id_stasiun.csv')
id_list = files_id['CODE'].tolist()

#df = pandas_gbq.read_gbq("SELECT * FROM Ma.Sensor LIMIT 10", credentials = credentials)
# Print results.
#st.write(df)

for df in id_list[:10]:
    ID = files_id[files_id['CODE']==df].index.values+11
    globals()[f'{df}']=pandas_gbq.read_gbq(f'SELECT * FROM Ma.Sensor where Station={int(ID)} LIMIT 10', credentials=credentials)
    globals() [f"{df}['logDate']"] = pd.to_datetime(globals() [f'{df}']['logDate']).dt.date
    
    globals() [f'pH_{df}'] = [x for x in globals()[f'{df}'][-24:]['pH']]
    globals() [f'ab_pH_{df}'] = sum(map(lambda x : x<5 and x>9, globals()[f'pH_{df}']))
    globals() [f'DO_{df}'] = [x for x in globals()[f'{df}'][-24:]['DO']]
    globals() [f'ab_DO_{df}'] = sum(map(lambda x : x<1, globals()[f'DO_{df}']))
    globals() [f'NH4_{df}'] = [x for x in globals()[f'{df}'][-24:]['NH4']]
    globals() [f'ab_NH4_{df}'] = sum(map(lambda x : x>100, globals()[f'NH4_{df}']))
    globals() [f'NO3_{df}'] = [x for x in globals()[f'{df}'][-24:]['NO3']]
    globals() [f'ab_NO3_{df}'] = sum(map(lambda x : x>100, globals()[f'NO3_{df}']))
    globals() [f'tgl_{df}'] = globals()[f'{df}']['logDate'].max()

def status_onlimo(id_ol):
    st.header(id_ol)
     globals()[f'header_a_{id_ol}'], globals()[f'header_b_{id_ol}'] = st.columns(2)
    

    if globals()[f'{id_ol}']['logDate'].max() == datetime.today().strftime('%Y-%m-%d'): 
        globals()[f'header_a_{id_ol}'].button(globals() [f'tgl_{id_ol}'], key=f'{id_ol}_a')
        globals()[f'header_b_{id_ol}'].success('ONLINE')

    elif globals()[f'tgl_{id_ol}'] < datetime.today().date(): 
        globals()[f'header_a_{id_ol}'].button(globals() [f'tgl_{id_ol}'].strftime("%Y-%m-%d"), key=f'{id_ol}_b')
        globals()[f'header_b_{id_ol}'].warning('OFFLINE')

    else:
        globals()[f'header_a_{id_ol}'].button(globals() [f'tgl_{id_ol}'], key=f'{id_ol}_b')                  
        globals()[f'header_b_{id_ol}'].error('ERROR')   

    col1, col2, col3, col4 = st.columns(4)
    col1.metric('pH', globals() [f'ab_pH_{id_ol}'], '{0:.2f}'. format(globals() [f'pH_{id_ol}'][-1]))
    col2.metric('DO', globals() [f'ab_DO_{id_ol}'], '{0:.2f}'. format(globals() [f'DO_{id_ol}'][-1]))
    col3.metric('NH', globals() [f'ab_NH4_{id_ol}'], '{0:.2f}'. format(globals() [f'NH4_{id_ol}'][-1]))
    col4.metric('NO', globals() [f'ab_NO3_{id_ol}'], '{0:.2f}'. format(globals() [f'NO3_{id_ol}'][-1]))

    st.markdown("""<hr style="height:5px;border:none;color:#333;background-color:#333;" /> """, unsafe_allow_html=True)

for x in id_list[:10]:
    status_onlimo(x)
#df = pd.DataFrame.from_dict(rows)
#st.write("Some wise words:")
#st.write(df)

table_id = 'onlimo.Ma.Record'
rows_to_insert = pd.DataFrame({u'Nama':'Waliy', u'Text':'Sukses'})
but = st.button('add data')
if but : 
    pandas_gbq.to_gbq(rows_to_insert, 'onlimo.Ma.Record', credentials=credentials)
