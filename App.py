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

rows = run_query("SELECT * FROM `Ma.Sensor` LIMIT 10")

#df = pandas.read_gbq("SELECT * FROM 'Ma.Sensor' LIMIT 10", project_id = "onlimo", credentials = credentials)
# Print results.
#st.write(df)

df = pd.DataFrame.from_dict(rows)
st.write("Some wise words:")
#st.write(df)

table_id = 'onlimo.Ma.Record'
rows_to_insert = [{u'Nama':'Waliy', u'Text':'Sukses'},]
but = st.button('add data')
if but:
    add = client.insert_rows_json(table_id, rows_to_insert)