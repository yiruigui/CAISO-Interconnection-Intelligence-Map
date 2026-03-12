import streamlit as st
import streamlit.components.v1 as components

st.set_page_config(
    page_title="CAISO Interconnection Intelligence Map",
    page_icon="🗺️",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# Remove all Streamlit chrome so the map is full-screen
st.markdown("""
<style>
    #root > div:first-child { height: 100vh; }
    .stApp { padding: 0 !important; margin: 0 !important; }
    .stApp > header { display: none !important; }
    .block-container { padding: 0 !important; max-width: 100% !important; }
    [data-testid="stAppViewContainer"] { padding: 0 !important; }
    [data-testid="stVerticalBlock"] { gap: 0 !important; }
    footer { display: none !important; }
</style>
""", unsafe_allow_html=True)

with open("caiso_tx_intelligence_v4.html", "r", encoding="utf-8") as f:
    html_content = f.read()

components.html(html_content, height=950, scrolling=False)
