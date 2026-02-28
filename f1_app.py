import streamlit as st
import subprocess
import duckdb
import database_connection as dc
import api_client as ac
import api_endpoints as ap
import json_polars_parser as parser
import data_models as dm
import helpers as hp
import altair as alt
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler
import pandas as pd
import api_gemini as llm

api_url = "https://api.jolpi.ca/ergast/f1"
api_client = ac.APIClient(base_url=api_url)
LOGO_URL_LARGE='./images/f1.png'
VIDEO='./images/f1_video.mov'

video_file = open(VIDEO, "rb")
video_bytes = video_file.read()

st.video(video_bytes,autoplay=True, muted=True)

st.title('F1 Application')

st.logo(LOGO_URL_LARGE, size="large")

def get_season_data_app(driverid):
    endpoint_location = ap.APIEndpoints(base_url=api_url, year=2010, limit=None, round=0, driverid=driverid, offset=0)
    endpoint = endpoint_location.get_seasons_endpoint_streamlit()

    data = api_client.fetch_data(endpoint=endpoint)
    total = hp.get_total_from_json(data)

    endpoint_location = ap.APIEndpoints(base_url=api_url, year=2010, limit=total, round=0, driverid=driverid, offset=0)
    endpoint = endpoint_location.get_seasons_endpoint_streamlit()
    data = api_client.fetch_data(endpoint=endpoint)

    data = parser.JSONPolarsParser(data)
    seasons_df = data.get_season_dataframe()
    seasons_df.write_delta('./landing_zone/streamlit/seasons/', mode='append')

@st.cache_data
def load_options():
    con = duckdb.connect("F1Data.db")
    drivers = con.execute("SELECT DISTINCT givenName || ' ' ||  familyName, driverid FROM drivers").fetchall()
    con.close()
    return drivers

@st.cache_data
def get_years_for_driver():
    con = duckdb.connect("F1Data.db")
    rows = con.execute("SELECT DISTINCT season FROM delta_scan('./landing_zone/streamlit/seasons/')").fetchall()
    return [row[0] for row in rows]

# Initialize session state
if "years" not in st.session_state:
    st.session_state.years = []
if "first_name" not in st.session_state:
    st.session_state.first_name = ""
if "family_name" not in st.session_state:
    st.session_state.family_name = ""
if "driverid" not in st.session_state:
    st.session_state.driverid = ""
if "process_app_state" not in st.session_state:
    st.session_state.process_app_state = 1
if "dbt_app_state" not in st.session_state:
    st.session_state.dbt_app_state = 1
 # Keep track of approval status
if "approved" not in st.session_state:
    st.session_state.approved = False


@st.dialog("Confirm approval")
def approval_dialog():
    st.write("Are you sure you want to delete the datalake and landing files")

    col1, col2 = st.columns(2)
    with col1:
        if st.button("✅ Approve"):
            st.session_state.approved = True
            result = hp.cleanup_from_streamlit()
            if result == 1:
                st.rerun()    # ✅ Close the dialog ONLY if successful
            else:
                st.error("❌ Unable to delete all files")
    with col2:
        if st.button("❌ Cancel"):
            st.rerun()


# 3. Show result on the main page
if st.session_state.approved:
    st.success("Datalake & Landing Zone Files Deleted ✅")

if st.button(f"Delete all data from Datalake and Landing Zone"):
    approval_dialog()

# Load drivers
driver_options = load_options()

# Driver selection
driver_placeholder = "Select a driver"
param1 = st.selectbox("Driver", driver_options)
name, driver_id = param1
# Button: Fetch and save seasons
if st.button(f"Get Seasons for {name}"):
    if name != driver_placeholder:
        hp.cleanup_streamlit()
        try:
            st.session_state.first_name, st.session_state.family_name = name.split(" ", 1)
            st.session_state.driverid = st.session_state.family_name.lower()
            get_season_data_app(driver_id)
            st.session_state.years = get_years_for_driver()
        except Exception as e:
            st.error(f"Error: {e}")

# Year selection
if st.session_state.years:
    year = st.selectbox("Select Year", st.session_state.years)
    st.write(f"You selected {name} for Season {year}")
else:
    st.warning("No seasons loaded. Please select a driver and click 'Get Seasons'.")

if st.button("Run F1 Script with Logs"):
    with st.status("Running script...", expanded=True):
        with st.spinner("Processing..."):

            # Start the external Python script
            process = subprocess.Popen(
                 ["python", "-u", "main.py", st.session_state.first_name, st.session_state.family_name, year, driver_id],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1
            )

            # Placeholder to show logs live
            log_placeholder = st.empty()
            log_lines = []

            # Read and display output line-by-line
            for line in process.stdout:
                log_lines.append(line)
                log_placeholder.text("".join(log_lines))

            process.wait()

        if process.returncode == 0:
            st.success("✅ Script completed successfully!")
            st.session_state.process_app_state=0
        else:
            st.error("❌ Script failed. Check the logs above.")
            st.session_state.process_app_state=1


if st.session_state.process_app_state == 1:
    # Button to trigger DBT
    if st.button("Run DBT Script"):
            with st.status("Running script...", expanded=True):
                with st.spinner("Running dbt..."):
                    # Run dbt command (adjust project path if needed)
                    result = subprocess.Popen(
                        ["dbt", "run", "--project-dir", "./f1_duckdb_project/"],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    bufsize=1
                    )
                # Placeholder to show logs live
                log_placeholder = st.empty()
                log_lines = []

                # Read and display output line-by-line
                for line in result.stdout:
                    log_lines.append(line)
                    log_placeholder.text("".join(log_lines))

                result.wait()

                # Show return code
                if result.returncode == 0:
                    st.session_state.dbt_app_state=0
                    st.success("DBT run completed successfully!")
                else:
                    st.session_state.dbt_app_state=1
                    st.error(f"DBT run failed with return code {result.returncode}")


user_query = st.text_input("Ask a question...")

if st.button("Submit"):
    if user_query.strip():
        st.write(f"You asked: {user_query}")
        sql_command=llm.llm_user_question(user_query)
        st.write(f"The system will run the following query: {sql_command}")
        result,summary=llm.llm_summarise(user_query,sql_command)
        st.write(f"SQL Result: {result}")
        st.write(f"The answer to your question is : {summary}")
    else:
        st.warning("Please type something before submitting.")