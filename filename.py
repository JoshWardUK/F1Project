import streamlit as st
import subprocess
import duckdb
import database_connection as dc
import api_client as ac
import api_endpoints as ap
import json_polars_parser as parser
import data_models as dm
import helpers as hp

api_url = "https://api.jolpi.ca/ergast/f1"
api_client = ac.APIClient(base_url=api_url)

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
    drivers = con.execute("SELECT DISTINCT givenName || ' ' ||  familyName FROM drivers").fetchall()
    con.close()
    return [row[0] for row in drivers]

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

# Load drivers
driver_options = load_options()

# Driver selection
driver_placeholder = "Select a driver"
param1 = st.selectbox("Driver", [driver_placeholder] + driver_options)

# Button: Fetch and save seasons
if st.button(f"Get Seasons for {param1}"):
    if param1 != driver_placeholder:
        hp.cleanup_streamlit()
        try:
            st.cache_data.clear()

            st.session_state.first_name, st.session_state.family_name = param1.split(" ", 1)
            st.session_state.driverid = st.session_state.family_name.lower()

            get_season_data_app(st.session_state.driverid)
            st.session_state.years = get_years_for_driver()
        except Exception as e:
            st.error(f"Error: {e}")

# Year selection
if st.session_state.years:
    year = st.selectbox("Select Year", st.session_state.years)
    st.write(f"You selected {st.session_state.first_name} {st.session_state.family_name} for Season {year}")
else:
    st.warning("No seasons loaded. Please select a driver and click 'Get Seasons'.")

if st.button("Run F1 Script with Logs"):
    with st.status("Running script...", expanded=True):
        with st.spinner("Processing..."):

            # Start the external Python script
            process = subprocess.Popen(
                 ["python", "-u", "main.py", st.session_state.first_name, st.session_state.family_name, year],
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
        else:
            st.error("❌ Script failed. Check the logs above.")



