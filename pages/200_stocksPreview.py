import streamlit as st
import datetime,pytz
import pandas as pd
from PIL import Image
from timeplus import *
import json

st.set_page_config(layout="wide")
col_img, col_txt, col_link = st.columns([1,8,5])
with col_img:
    image = Image.open("detailed-analysis@2x.png")
    st.image(image, width=100)
with col_txt:
    st.title("Real-time Insights for Stocks via Confluent Cloud")
with col_link:
    st.markdown("[Source Code](https://github.com/esha0612/dashboard/edit/203dashboard/pages/200_stocksPreview.py)", unsafe_allow_html=True)
    
env = Environment().address("https://us.timeplus.cloud").apikey("aV9q9Fz6uMhBK9TaGoh9iFdvowFnlVa3gavnoK8vEiSvKS1kHTo4YkxMDc2G").workspace("st3o6qm2")    

MAX_ROW=10
st.session_state.rows=0
sql='SELECT _tp_time,symbol, userid, quantity, price, side FROM stocksEsha'
st.code(sql, language="sql")
with st.empty():
    query = Query(env=env).sql(query=sql).create()
    col = [h["name"] for h in query.metadata()["result"]["header"]]
    def update_row(row,name):
        data = {}
        for i, f in enumerate(col):
            data[f] = row[i]
          

        df = pd.DataFrame([data], columns=col)
        st.session_state.rows=st.session_state.rows+1
        if name not in st.session_state or st.session_state.rows>=MAX_ROW:
            st.session_state[name] = st.table(df)
            st.session_state.rows=0
        else:
            st.session_state[name].add_rows(df)
    # iterate query result
    limit = MAX_ROW*10-1
    count = 0
    for event in query.result():
        if event.event != "metrics" and event.event != "query":
            for row in json.loads(event.data):
                update_row(row,"tail")
                count += 1
                if count >= limit:
                    break
            # break the outer loop too    
            if count >= limit:
                break                
    query.cancel()
    query.delete()

st.write(f"Only the recent {MAX_ROW*10} rows are shown. You can refresh the page to view the latest events.")

