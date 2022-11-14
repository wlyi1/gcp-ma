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

df = pandas_gbq.read_gbq("SELECT * FROM Ma.Sensor LIMIT 10", credentials = credentials)
# Print results.
st.write(df)

for i in id_list[:10]:
    ID = files_id[files_id['CODE']==i].index.values+11
    globals()[f'query_{i}']=pandas_qbg.read_gbq(f'select * from Ma.Sensor LIMIT 10 where Station={int(ID)}')

def status_onlimo(id_ol):
    st.header(id_ol)
    st.write(globals() [f'query_{id_ol}'])

for x in id_list[:10]:
    status_onlimo(x)
#df = pd.DataFrame.from_dict(rows)
#st.write("Some wise words:")
#st.write(df)

table_id = 'onlimo.Ma.Record'
rows_to_insert = [{u'Nama':'Waliy', u'Text':'Sukses'},]
but = st.button('add data')

