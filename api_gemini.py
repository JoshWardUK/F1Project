from google import genai
import json
import os
import duckdb
import pandas as pd


# The client gets the API key from the environment variable `GEMINI_API_KEY`.
client = genai.Client()

"""
################################################################################

LLM - Ask Germini a Question about the F1 Data

################################################################################
"""
def llm_user_question(user_question):


    TABLES = [
    ("bronze", "bronze_circuits"),
    ("bronze", "bronze_constructorstandings"),
    ("bronze", "bronze_driver_standings"),
    ("bronze", "bronze_drivers"),
    ("bronze", "bronze_laps"),
    ("bronze", "bronze_pitstops"),
    ("bronze", "bronze_qualifying"),
    ("bronze", "bronze_races"),
    ("bronze", "bronze_results"),
    ("bronze", "bronze_seasons"),
    ("bronze", "bronze_sprint"),
    ("silver", "dim_circuits"),
    ("silver", "dim_constructor_standings"),
    ("silver", "dim_constructors"),
    ("silver", "dim_driver_standings"),
    ("silver", "dim_drivers"),
    ("silver", "fct_lap_times"),
    ("silver", "fct_pit_stops"),
    ("silver", "fct_qualifying"),
    ("silver", "dim_races"),
    ("silver", "fct_results"),
    ("silver", "silver_results"),
    ("silver", "fct_sprint_results"),
    ("gold", "driver_season_race_results"),
    ]

    con = duckdb.connect(":memory:")

    # 2. Load extension
    con.execute("""ATTACH 'ducklake:f1_duckdb_project/F1metadata.ducklake' AS datalake
        (DATA_PATH '/Users/joshuaward/Documents/Data-Engineering/F1Project/F1Project/f1_duckdb_project/data/');""")

    con.execute("use datalake;")

    # ---------- 1. COMBINED DATA ----------
    data_frames = []

    for schema, table in TABLES:
        query = f"""
            SELECT
                '{schema}' AS schema_name,
                '{table}'  AS table_name,
                *
            FROM {schema}.{table}
        """
        df = con.execute(query).df()
        data_frames.append(df)

    # "Wide" combined DataFrame of all rows from all tables
    all_data_df = pd.concat(data_frames, ignore_index=True)

    # ---------- 2. COMBINED DESCRIBE / SCHEMA ----------
    schema_frames = []

    for schema, table in TABLES:
        describe_query = f"DESCRIBE {schema}.{table}"
        df_desc = con.execute(describe_query).df()
        df_desc["schema_name"] = schema
        df_desc["table_name"] = table
        schema_frames.append(df_desc)

    # Combined schema/metadata DataFrame
    all_schema_df = pd.concat(schema_frames, ignore_index=True)

    con.close()

    data_payload = all_data_df.to_dict(orient="records")
    schema_payload = all_schema_df.to_dict(orient="records")

    #Build the prompt
    system_instructions_question = (
        f"""
        You are to only return the DuckDB SQL command for the users request in plain text - dont include anything other than the sql command that can be used directly by duckdb
        Dont include any ``` in the output.
        Below is the table descriptions:

        Schema Payload:
        {schema_payload}

        Only return ansi sql please - in duckdb compatiable sql!

        Dont do any data type conversions!

   
        """
    )

    response = client.models.generate_content(
        model="gemini-2.5-flash",  # or "gemini-2.5-pro" for heavier reasoning
        contents=[
            {"role": "user", "parts": [
                {"text": system_instructions_question},
                {"text": user_question}
            ]}
        ]
    )
    sql_command = response.text
    print(sql_command)
    return sql_command

"""
################################################################################

LLM - Ask Germini to supply the user with a detailed answer. 

################################################################################
"""

def llm_summarise(user_question,sql_command):
    # 1. Connect
    con = duckdb.connect(":memory:")

    # 2. Load extension
    con.execute("""ATTACH 'ducklake:f1_duckdb_project/F1metadata.ducklake' AS datalake
        (DATA_PATH '/Users/joshuaward/Documents/Data-Engineering/F1Project/F1Project/f1_duckdb_project/data/');""")

    con.execute("use datalake;")

    df = con.execute(sql_command).fetch_df()
    records = df.to_dict(orient="records")
    json_str = json.dumps(records, indent=2)

    con.close()

    system_instructions_question = ("The user has asked the below question. "
    "Create a summary using the question and the response which is below. Write it in a formula 1 commentator format ")

    response = client.models.generate_content(
        model="gemini-2.5-flash",  # or "gemini-2.5-pro" for heavier reasoning
        contents=[
            {"role": "user", "parts": [
                {"text": system_instructions_question+user_question+json_str}
            ]}
        ]
    )

    summary = response.text
    return json_str,summary
