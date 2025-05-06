import streamlit as st
import subprocess

st.title("F1 Analysis Logger")

if st.button("Run F1 Script with Logs"):
    with st.status("Running script...", expanded=True):
        with st.spinner("Processing..."):

            # Start the external Python script
            process = subprocess.Popen(
                 ["python", "-u", "main.py"],
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