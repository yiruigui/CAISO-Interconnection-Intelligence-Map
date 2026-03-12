import streamlit as st
import streamlit.components.v1 as components

st.set_page_config(
    page_title="CAISO Interconnection Intelligence Map",
    page_icon="🗺️",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# Nuke every pixel of Streamlit chrome so the iframe is flush to the browser window
st.markdown("""
<style>
    /* Hide header, toolbar, footer, decoration */
    header[data-testid="stHeader"]          { display: none !important; }
    [data-testid="stToolbar"]               { display: none !important; }
    [data-testid="stDecoration"]            { display: none !important; }
    [data-testid="stStatusWidget"]          { display: none !important; }
    footer                                  { display: none !important; }

    /* Zero out ALL padding/margin on every Streamlit wrapper */
    html, body                              { margin: 0; padding: 0; overflow: hidden; height: 100%; }
    .stApp                                  { margin: 0 !important; padding: 0 !important; }
    .main                                   { padding: 0 !important; overflow: hidden !important; }
    .block-container                        { padding: 0 !important; margin: 0 !important;
                                              max-width: 100% !important; min-width: 100% !important; }
    [data-testid="stAppViewContainer"]      { padding: 0 !important; margin: 0 !important; }
    [data-testid="stAppViewBlockContainer"] { padding: 0 !important; }
    [data-testid="stVerticalBlock"]         { gap: 0 !important; padding: 0 !important; }
    [data-testid="stMainBlockContainer"]    { padding: 0 !important; }

    /* Make the iframe borderless and block-level */
    iframe { border: none !important; display: block !important; }
</style>
""", unsafe_allow_html=True)

with open("caiso_tx_intelligence_v4.html", "r", encoding="utf-8") as f:
    html_content = f.read()

# Height = full viewport. scrolling=True lets the inner sidebar scroll freely.
components.html(html_content, height=1080, scrolling=True)
