import streamlit as st
import subprocess
import duckdb

st.title("F1 Analysis Application")

@st.cache_data
def load_options():
    con = duckdb.connect("F1Data.db")
    drivers = con.execute("SELECT DISTINCT givenName || ' ' ||  familyName FROM drivers").fetchall()
    season = con.execute("SELECT DISTINCT Season FROM Seasons").fetchall()
    con.close()
    return [row[0] for row in drivers], [row[0] for row in season]

# Use cached data
driver_options, season_options = load_options()

# Dropdowns
param1 = st.selectbox("Select Driver", driver_options)
param2 = st.selectbox("Select Season", season_options)

if st.button("Run F1 Script with Logs"):
    with st.status("Running script...", expanded=True):
        with st.spinner("Processing..."):

            # Start the external Python script
            process = subprocess.Popen(
                 ["python", "-u", "main.py", param1, param2],
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