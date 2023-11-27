import time
import duckdb
import pandas as pd
import streamlit as st

con = duckdb.connect(database='kubernetes.duckdb', read_only=True)
st.set_page_config(layout="wide")
st.title('Kubernetes monitor')

@st.cache_data
def get_data() -> pd.DataFrame:
    stmt = """
SELECT 
    pod_ip,
    name,
    status,
    namespace
FROM kubernetes_data.kubernetes_resource
    """

    return con.execute(stmt).df()

df = get_data()
placeholder = st.empty()

with placeholder.container():
    c1, c2 = st.columns(2)

    with c1:
        st.metric('Total pods', df['name'].count())

    with c2:
        st.metric('Unique namespaces', df['namespace'].nunique())

    namespaces = st.selectbox('Namespaces', df['namespace'].unique())
    st.table(df[df['namespace'] == namespaces])
    time.sleep(1)
