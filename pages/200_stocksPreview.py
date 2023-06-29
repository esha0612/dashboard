import streamlit as st
import time,datetime,pytz,os,json
import pandas as pd
import streamlit as st
import altair as alt
from PIL import Image

from timeplus import *

col_img, col_txt, col_link = st.columns([2,6,2])
with col_img:
    image = Image.open("detailed-analysis@2x.png")
    st.image(image, width=100)
with col_txt:
    st.title("Real-time Insights for Stocks via Confluent Cloud")
with col_link:
    st.markdown("[Source Code](https://github.com/esha0612/dashboard/edit/203dashboard/pages/200_stocksPreview.py)", unsafe_allow_html=True)

tab2, tab1 = st.tabs(["Realtime Data", "Event Count + Insights"])
env = Environment().address(st.secrets["db_address"]).apikey(st.secrets["db_apikey"]).workspace(st.secrets["db_workspace"])    
def batchQuery(bathSQL):
    q=Query(env=env).sql(query=bathSQL).create()
    header=q.metadata()["result"]["header"]
    rows=[]
    for event in q.result():
        if event.event != "metrics" and event.event != "query":
            for row in json.loads(event.data):
                rows.append(row)
    q.cancel()
    q.delete()
    return header,rows
def show_table_for_query(sql,table_name,row_cnt):
    st.code(sql, language="sql")
    query = Query(env=env).sql(query=sql).create()
    col = [h["name"] for h in query.metadata()["result"]["header"]]
    def update_table(row,name):
        data = {}
        for i, f in enumerate(col):
            data[f] = row[i]
            #hack show first column as more friendly datetime diff
           
        df = pd.DataFrame([data], columns=col)
        if name not in st.session_state:
            st.session_state[name] = st.table(df)
        else:
            st.session_state[name].add_rows(df)
    # iterate query result
    limit = row_cnt
    count = 0
    for event in query.result():
        if event.event != "metrics" and event.event != "query":
            for row in json.loads(event.data):
                update_table(row,table_name)
                count += 1
                if count >= limit:
                    break
            # break the outer loop too    
            if count >= limit:
                break
    query.cancel()
    query.delete()

with tab2:
    #st.header('New repos')
    #show_table_for_query("""SELECT created_at,actor,repo,json_extract_string(payload,'master_branch') AS branch \nFROM github_events WHERE type='CreateEvent'""",'new_repo',3)

    MAX_ROW=5
    st.session_state.rows= 4
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
        limit = MAX_ROW*1
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

    st.write(f"Only the recent {MAX_ROW*1} rows are shown. You can refresh the page to view the latest events.")
with tab1:
    
    
    st.header('New events every minute')

# SQL query
    sql = "SELECT window_end AS time, symbol, sum(quantity) AS count from tumble(table(stocksEsha),5m) WHERE _tp_time > date_sub(now(), 2h) GROUP BY window_end, symbol ORDER BY 1, 3 DESC"
    st.code(sql, language="sql")
    result = batchQuery(sql)

# Convert the result to a DataFrame
    col = [h["name"] for h in result[0]]
    df = pd.DataFrame(result[1], columns=col)

# Create the bar chart using Altair
    c = alt.Chart(df).mark_bar().encode(
        x='time:T',
        y='count:Q',
        tooltip=['count'],
        color=alt.value('#D53C97')
    )

# Display the bar chart
    st.altair_chart(c, use_container_width=True)
    st.header("Hot Stocks")
    sql="SELECT symbol, sum(quantity) AS total_quantity FROM stocksEsha GROUP BY symbol ORDER BY total_quantity DESC LIMIT 10;"
    show_table_for_query(sql,'star_table',5)
with tab1:

    st.header('Event count')
    st.code("SELECT count(*) FROM stocksEsha EMIT periodic 1s", language="sql")
    with st.empty():
        #show the initial events first
        sql="select count(*) from table(stocksEsha)"
        cnt=batchQuery(sql)[1][0][0]
        st.metric(label="Stocks events", value="{:,}".format(cnt))
        st.session_state.last_cnt=cnt

        #create a streaming query to update counts
        sql=f"select {cnt}+count(*) as events from stocksEsha emit periodic 1s"
        query = Query(env=env).sql(query=sql).create()
        def update_row(row):
            delta=row[0]-st.session_state.last_cnt
            if (delta>0):
                st.metric(label="Stocks events", value="{:,}".format(row[0]), delta=row[0]-st.session_state.last_cnt, delta_color='inverse')
                st.session_state.last_cnt=row[0]
        # iterate query result
        limit = 200
        count = 0
        for event in query.result():
            if event.event != "metrics" and event.event != "query":
                for row in json.loads(event.data):
                    update_row(row)
                    count += 1
                    if count >= limit:
                        break
                # break the outer loop too    
                if count >= limit:
                    break
        query.cancel()
        query.delete()
