from google import genai
import json
import os
import duckdb


# The client gets the API key from the environment variable `GEMINI_API_KEY`.
client = genai.Client()

# 3. Build your prompt
system_instructions = (
    "You are an expert Formula 1 race analyst. "
    "Given structured race data, explain what happened in clear, engaging language."
    "Avoid fabricating details that are not supported by the data."
)

user_prompt = (
    "Please summarise all the laps for a casual F1 fan. "
    "Explain each lap and any interesting patterns. "
    "Keep it under 400 words."
)

prompt = ("""You are an elite Formula 1 race engineer and performance analyst.

You will be given a JSON dataset containing lap-by-lap race timing information
for a single driver across one or more races. Each lap entry includes:

- lap number
- lap_time_seconds (float)
- position (int)
- stint (if provided)
- tyre compound (if provided)
- tyre life (if provided)
- sector times (if provided)
- pit in/out times (if provided)

Your task is:

1. **Summarise the race lap-by-lap in grouped phases**
   - Do NOT list every lap one by one.
   - Instead, group laps into meaningful performance segments, e.g.:
       • Opening laps (Laps 1–5)
       • Early-stint pace (Laps 6–14)
       • Tyre drop-off phases
       • Push laps / recovery laps
       • Pit-stop in-lap / out-lap windows
       • Safety-car-like slow phases (if lap times spike sharply)
   - For each phase, describe:
       • pace trend (improving? degrading?)
       • tyre behaviour
       • traffic/position changes
       • notable spikes or dips in lap time
       • possible causes based purely on data

2. **Identify key moments**
   - the three fastest laps & where they occurred
   - the three slowest laps & likely reasons (e.g., degradation, pit in-lap)
   - biggest positional gain or loss windows
   - any anomalies or pace cliffs

3. **Pit-stop analysis (if pit_in_time / pit_out_time exists)**
   - evaluate the in-lap pace vs surrounding laps
   - evaluate the out-lap pace compared to tyre life and compound
   - highlight whether pit windows were optimal based on timing data

4. **Tyre behaviour analysis**
   - describe how pace changed with tyre life
   - identify degradation slopes (e.g., “lap time increases ~0.12s per lap on Mediums”)
   - compare stints if multiple exist

5. **Race strategy interpretation**
   - infer whether the driver was pushing, defending, saving tyres, or struggling
   - based ONLY on timing + position + tyre life/compound
   - do not invent real-world events not present in the data

6. **Final structured output**
   Provide:
   - a high-level race summary (4–6 sentences)
   - a detailed phase-by-phase analysis
   - bullet points of key takeaways for an engineer
   - a fan-friendly one-paragraph summary

Rules:
- Base everything strictly on the JSON data. 
- Do NOT hallucinate events that are not present in the data.
- If something cannot be known from the data, explicitly state that.
- Speak in clear, analytical language like an F1 performance engineer.

Here is the JSON dataset:""")

# 1. Connect
#con = duckdb.connect(":memory:")

# 2. Load extension
#con.execute("""ATTACH 'ducklake:F1metadata.ducklake' AS datalake
#    (DATA_PATH '/Users/joshuaward/Documents/Data-Engineering/F1Project/F1Project/f1_duckdb_project/data/');""")

#con.execute("use datalake;")


#df = con.execute("""select *
#from bronze.bronze_laps
#where season = '2024'
#and raceName = 'Las Vegas Grand Prix'
#and driverId = 'hamilton'
#order by lapNumber::int;""").fetch_df()
#records = df.to_dict(orient="records")
#json_str = json.dumps(records, indent=2)


# 4. Call Gemini 2.5 with text + JSON
"""
response = client.models.generate_content(
    model="gemini-2.5-flash",  # or "gemini-2.5-pro" for heavier reasoning
    contents=[
        {"role": "user", "parts": [
            {"text": system_instructions},
            {"text": user_prompt},
            {"text": "Here is the race data in JSON format:"},
            {"text": json_str}
        ]}
    ]
)
"""


# 5. Extract the summary text
#summary = response.text
#print(summary)

audio_prompts = """You are a high-energy Formula 1 TV commentator who speaks in a British accent

Your job:

1. Turn it into an exciting spoken commentary script that could be read over a race highlights video.
2. Use an F1 TV style: urgent, punchy, emotional. Think Sky Sports F1 / Crofty style.
"""

#audio_prompt = audio_prompts + ' here is what you should say:' + summary

from google import genai
from google.genai import types
import wave

# Set up the wave file to save the output:
def wave_file(filename, pcm, channels=1, rate=24000, sample_width=2):
   with wave.open(filename, "wb") as wf:
      wf.setnchannels(channels)
      wf.setsampwidth(sample_width)
      wf.setframerate(rate)
      wf.writeframes(pcm)

client = genai.Client()

"""
response = client.models.generate_content(
   model="gemini-2.5-flash-preview-tts",
   contents=audio_prompt,
   config=types.GenerateContentConfig(
      response_modalities=["AUDIO"],
      speech_config=types.SpeechConfig(
         voice_config=types.VoiceConfig(
            prebuilt_voice_config=types.PrebuiltVoiceConfig(
               voice_name='Fenrir',
            )
         )
      ),
   )
)
"""
#data = response.candidates[0].content.parts[0].inline_data.data

#file_name='out.wav'
#wave_file(file_name, data) # Saves the file to current directory



"""
################################################################################

LLM - Ask Germini a Question about the F1 Data

################################################################################
"""

#Build the prompt
system_instructions_question = (
    """You are to only return the DuckDB SQL command for the users request. Only return the SQL Command in plain text. Include the schema name e.g. bronze tables are in the bronze schema. Use the following metadata to formulate the query:
column_id	begin_snapshot	end_snapshot	table_id	column_order	column_name	column_type	initial_default	default_value	nulls_allowed	parent_column	table_name	schema_name
1	4		4	1	circuitId	varchar			true		bronze_circuits	bronze
2	4		4	2	circuitName	varchar			true		bronze_circuits	bronze
3	4		4	3	url	varchar			true		bronze_circuits	bronze
4	4		4	4	locality	varchar			true		bronze_circuits	bronze
5	4		4	5	country	varchar			true		bronze_circuits	bronze
6	4		4	6	latitude	varchar			true		bronze_circuits	bronze
7	4		4	7	longitude	varchar			true		bronze_circuits	bronze
1	5		5	1	season	varchar			true		bronze_constructorstandings	bronze
2	5		5	2	round	varchar			true		bronze_constructorstandings	bronze
3	5		5	3	position	varchar			true		bronze_constructorstandings	bronze
4	5		5	4	positionText	varchar			true		bronze_constructorstandings	bronze
5	5		5	5	points	varchar			true		bronze_constructorstandings	bronze
6	5		5	6	wins	varchar			true		bronze_constructorstandings	bronze
7	5		5	7	constructorId	varchar			true		bronze_constructorstandings	bronze
8	5		5	8	constructorName	varchar			true		bronze_constructorstandings	bronze
9	5		5	9	constructorNationality	varchar			true		bronze_constructorstandings	bronze
10	5		5	10	constructorUrl	varchar			true		bronze_constructorstandings	bronze
1	6		6	1	season	varchar			true		bronze_driver_standings	bronze
2	6		6	2	round	varchar			true		bronze_driver_standings	bronze
3	6		6	3	position	varchar			true		bronze_driver_standings	bronze
4	6		6	4	positionText	varchar			true		bronze_driver_standings	bronze
5	6		6	5	points	varchar			true		bronze_driver_standings	bronze
6	6		6	6	wins	varchar			true		bronze_driver_standings	bronze
7	6		6	7	driverId	varchar			true		bronze_driver_standings	bronze
8	6		6	8	driverName	varchar			true		bronze_driver_standings	bronze
9	6		6	9	driverNationality	varchar			true		bronze_driver_standings	bronze
10	6		6	10	driverDOB	varchar			true		bronze_driver_standings	bronze
11	6		6	11	driverCode	varchar			true		bronze_driver_standings	bronze
12	6		6	12	driverUrl	varchar			true		bronze_driver_standings	bronze
13	6		6	13	constructorId	varchar			true		bronze_driver_standings	bronze
14	6		6	14	constructorName	varchar			true		bronze_driver_standings	bronze
15	6		6	15	constructorNationality	varchar			true		bronze_driver_standings	bronze
16	6		6	16	constructorUrl	varchar			true		bronze_driver_standings	bronze
1	7		7	1	season	varchar			true		bronze_drivers	bronze
2	7		7	2	round	varchar			true		bronze_drivers	bronze
3	7		7	3	position	varchar			true		bronze_drivers	bronze
4	7		7	4	positionText	varchar			true		bronze_drivers	bronze
5	7		7	5	points	varchar			true		bronze_drivers	bronze
6	7		7	6	wins	varchar			true		bronze_drivers	bronze
7	7		7	7	driverId	varchar			true		bronze_drivers	bronze
8	7		7	8	givenName	varchar			true		bronze_drivers	bronze
9	7		7	9	familyName	varchar			true		bronze_drivers	bronze
10	7		7	10	dateOfBirth	varchar			true		bronze_drivers	bronze
11	7		7	11	driverNationality	varchar			true		bronze_drivers	bronze
12	7		7	12	constructorId	varchar			true		bronze_drivers	bronze
13	7		7	13	constructorName	varchar			true		bronze_drivers	bronze
14	7		7	14	constructorNationality	varchar			true		bronze_drivers	bronze
1	8		8	1	season	varchar			true		bronze_laps	bronze
2	8		8	2	round	varchar			true		bronze_laps	bronze
3	8		8	3	raceName	varchar			true		bronze_laps	bronze
4	8		8	4	circuitId	varchar			true		bronze_laps	bronze
5	8		8	5	circuitName	varchar			true		bronze_laps	bronze
6	8		8	6	raceDate	varchar			true		bronze_laps	bronze
7	8		8	7	lapNumber	varchar			true		bronze_laps	bronze
8	8		8	8	driverId	varchar			true		bronze_laps	bronze
9	8		8	9	position	varchar			true		bronze_laps	bronze
10	8		8	10	lapTime	varchar			true		bronze_laps	bronze
1	9		9	1	season	varchar			true		bronze_pitstops	bronze
2	9		9	2	round	varchar			true		bronze_pitstops	bronze
3	9		9	3	raceName	varchar			true		bronze_pitstops	bronze
4	9		9	4	circuitId	varchar			true		bronze_pitstops	bronze
5	9		9	5	circuitName	varchar			true		bronze_pitstops	bronze
6	9		9	6	date	varchar			true		bronze_pitstops	bronze
7	9		9	7	time	varchar			true		bronze_pitstops	bronze
8	9		9	8	driverId	varchar			true		bronze_pitstops	bronze
9	9		9	9	lap	varchar			true		bronze_pitstops	bronze
10	9		9	10	stop	varchar			true		bronze_pitstops	bronze
11	9		9	11	pitTime	varchar			true		bronze_pitstops	bronze
12	9		9	12	duration	varchar			true		bronze_pitstops	bronze
1	10		10	1	season	varchar			true		bronze_qualifying	bronze
2	10		10	2	round	varchar			true		bronze_qualifying	bronze
3	10		10	3	raceName	varchar			true		bronze_qualifying	bronze
4	10		10	4	circuitId	varchar			true		bronze_qualifying	bronze
5	10		10	5	circuitName	varchar			true		bronze_qualifying	bronze
6	10		10	6	raceDate	varchar			true		bronze_qualifying	bronze
7	10		10	7	driverId	varchar			true		bronze_qualifying	bronze
8	10		10	8	code	varchar			true		bronze_qualifying	bronze
9	10		10	9	givenName	varchar			true		bronze_qualifying	bronze
10	10		10	10	familyName	varchar			true		bronze_qualifying	bronze
11	10		10	11	constructor	varchar			true		bronze_qualifying	bronze
12	10		10	12	constructorId	varchar			true		bronze_qualifying	bronze
13	10		10	13	position	varchar			true		bronze_qualifying	bronze
14	10		10	14	Q1	varchar			true		bronze_qualifying	bronze
15	10		10	15	Q2	varchar			true		bronze_qualifying	bronze
16	10		10	16	Q3	varchar			true		bronze_qualifying	bronze
1	11		11	1	season	varchar			true		bronze_races	bronze
2	11		11	2	round	varchar			true		bronze_races	bronze
3	11		11	3	raceName	varchar			true		bronze_races	bronze
4	11		11	4	circuitId	varchar			true		bronze_races	bronze
5	11		11	5	circuitName	varchar			true		bronze_races	bronze
6	11		11	6	lat	varchar			true		bronze_races	bronze
7	11		11	7	long	varchar			true		bronze_races	bronze
8	11		11	8	locality	varchar			true		bronze_races	bronze
9	11		11	9	country	varchar			true		bronze_races	bronze
10	11		11	10	raceDate	varchar			true		bronze_races	bronze
11	11		11	11	raceTime	varchar			true		bronze_races	bronze
12	11		11	12	fp1Date	varchar			true		bronze_races	bronze
13	11		11	13	fp1Time	varchar			true		bronze_races	bronze
14	11		11	14	fp2Date	varchar			true		bronze_races	bronze
15	11		11	15	fp2Time	varchar			true		bronze_races	bronze
16	11		11	16	fp3Date	varchar			true		bronze_races	bronze
17	11		11	17	fp3Time	varchar			true		bronze_races	bronze
18	11		11	18	qualifyingDate	varchar			true		bronze_races	bronze
19	11		11	19	qualifyingTime	varchar			true		bronze_races	bronze
20	11		11	20	sprintDate	varchar			true		bronze_races	bronze
21	11		11	21	sprintTime	varchar			true		bronze_races	bronze
1	12		12	1	season	varchar			true		bronze_results	bronze
2	12		12	2	round	varchar			true		bronze_results	bronze
3	12		12	3	raceName	varchar			true		bronze_results	bronze
4	12		12	4	circuitId	varchar			true		bronze_results	bronze
5	12		12	5	circuitName	varchar			true		bronze_results	bronze
6	12		12	6	date	varchar			true		bronze_results	bronze
7	12		12	7	time	varchar			true		bronze_results	bronze
8	12		12	8	driverId	varchar			true		bronze_results	bronze
9	12		12	9	driverName	varchar			true		bronze_results	bronze
10	12		12	10	constructorId	varchar			true		bronze_results	bronze
11	12		12	11	constructorName	varchar			true		bronze_results	bronze
12	12		12	12	grid	varchar			true		bronze_results	bronze
13	12		12	13	position	varchar			true		bronze_results	bronze
14	12		12	14	points	varchar			true		bronze_results	bronze
15	12		12	15	laps	varchar			true		bronze_results	bronze
16	12		12	16	status	varchar			true		bronze_results	bronze
17	12		12	17	fastestLap	varchar			true		bronze_results	bronze
18	12		12	18	fastestLapTime	varchar			true		bronze_results	bronze
19	12		12	19	averageSpeed	varchar			true		bronze_results	bronze
1	13		13	1	season	varchar			true		bronze_seasons	bronze
2	13		13	2	url	varchar			true		bronze_seasons	bronze
1	14		14	1	season	varchar			true		bronze_sprint	bronze
2	14		14	2	round	varchar			true		bronze_sprint	bronze
3	14		14	3	raceName	varchar			true		bronze_sprint	bronze
4	14		14	4	circuitId	varchar			true		bronze_sprint	bronze
5	14		14	5	circuitName	varchar			true		bronze_sprint	bronze
6	14		14	6	raceDate	varchar			true		bronze_sprint	bronze
7	14		14	7	raceTime	varchar			true		bronze_sprint	bronze
8	14		14	8	driverId	varchar			true		bronze_sprint	bronze
9	14		14	9	code	varchar			true		bronze_sprint	bronze
10	14		14	10	givenName	varchar			true		bronze_sprint	bronze
11	14		14	11	familyName	varchar			true		bronze_sprint	bronze
12	14		14	12	constructor	varchar			true		bronze_sprint	bronze
13	14		14	13	constructorId	varchar			true		bronze_sprint	bronze
14	14		14	14	grid	varchar			true		bronze_sprint	bronze
15	14		14	15	position	varchar			true		bronze_sprint	bronze
16	14		14	16	positionText	varchar			true		bronze_sprint	bronze
17	14		14	17	points	varchar			true		bronze_sprint	bronze
18	14		14	18	laps	varchar			true		bronze_sprint	bronze
19	14		14	19	status	varchar			true		bronze_sprint	bronze
20	14		14	20	sprintTime	varchar			true		bronze_sprint	bronze
21	14		14	21	sprintTimeMillis	varchar			true		bronze_sprint	bronze
22	14		14	22	fastestLap	varchar			true		bronze_sprint	bronze
23	14		14	23	fastestLapTime	varchar			true		bronze_sprint	bronze
1	16		16	1	circuit_id	varchar			true		dim_circuits	silver
2	16		16	2	circuit_name	varchar			true		dim_circuits	silver
3	16		16	3	url	varchar			true		dim_circuits	silver
4	16		16	4	city	varchar			true		dim_circuits	silver
5	16		16	5	country	varchar			true		dim_circuits	silver
6	16		16	6	latitude	float64			true		dim_circuits	silver
7	16		16	7	longitude	float64			true		dim_circuits	silver
1	17		17	1	season_year	int32			true		dim_constructor_standings	silver
2	17		17	2	round	int32			true		dim_constructor_standings	silver
3	17		17	3	standing_position	int32			true		dim_constructor_standings	silver
4	17		17	4	position_text	varchar			true		dim_constructor_standings	silver
5	17		17	5	points	float64			true		dim_constructor_standings	silver
6	17		17	6	wins	int32			true		dim_constructor_standings	silver
7	17		17	7	constructor_id	varchar			true		dim_constructor_standings	silver
8	17		17	8	constructor_name	varchar			true		dim_constructor_standings	silver
9	17		17	9	constructor_nationality	varchar			true		dim_constructor_standings	silver
1	18		18	1	constructor_id	varchar			true		dim_constructors	silver
2	18		18	2	constructor_name	varchar			true		dim_constructors	silver
3	18		18	3	constructor_nationality	varchar			true		dim_constructors	silver
4	18		18	4	season_year	int32			true		dim_constructors	silver
5	18		18	5	round	int32			true		dim_constructors	silver
1	19		19	1	season_year	int32			true		dim_driver_standings	silver
2	19		19	2	round	int32			true		dim_driver_standings	silver
3	19		19	3	standing_position	int32			true		dim_driver_standings	silver
4	19		19	4	standing_position_text	varchar			true		dim_driver_standings	silver
5	19		19	5	points	float64			true		dim_driver_standings	silver
6	19		19	6	wins	int32			true		dim_driver_standings	silver
7	19		19	7	driver_id	varchar			true		dim_driver_standings	silver
8	19		19	8	driver_name	varchar			true		dim_driver_standings	silver
9	19		19	9	driver_nationality	varchar			true		dim_driver_standings	silver
10	19		19	10	driver_dob	date			true		dim_driver_standings	silver
11	19		19	11	driver_code	varchar			true		dim_driver_standings	silver
12	19		19	12	driver_url	varchar			true		dim_driver_standings	silver
13	19		19	13	constructor_id	varchar			true		dim_driver_standings	silver
14	19		19	14	constructor_name	varchar			true		dim_driver_standings	silver
15	19		19	15	constructor_nationality	varchar			true		dim_driver_standings	silver
16	19		19	16	constructor_url	varchar			true		dim_driver_standings	silver
1	20		20	1	driver_id	varchar			true		dim_drivers	silver
2	20		20	2	given_name	varchar			true		dim_drivers	silver
3	20		20	3	family_name	varchar			true		dim_drivers	silver
4	20		20	4	date_of_birth	date			true		dim_drivers	silver
5	20		20	5	nationality	varchar			true		dim_drivers	silver
6	20		20	6	constructor_id	varchar			true		dim_drivers	silver
7	20		20	7	constructor_name	varchar			true		dim_drivers	silver
8	20		20	8	constructor_nationality	varchar			true		dim_drivers	silver
9	20		20	9	season_year	int32			true		dim_drivers	silver
10	20		20	10	round	int32			true		dim_drivers	silver
11	20		20	11	standing_position	int32			true		dim_drivers	silver
12	20		20	12	standing_position_text	varchar			true		dim_drivers	silver
13	20		20	13	points	float64			true		dim_drivers	silver
14	20		20	14	wins	int32			true		dim_drivers	silver
1	21		21	1	season_year	int32			true		fct_lap_times	silver
2	21		21	2	round	int32			true		fct_lap_times	silver
3	21		21	3	race_name	varchar			true		fct_lap_times	silver
4	21		21	4	circuit_id	varchar			true		fct_lap_times	silver
5	21		21	5	circuit_name	varchar			true		fct_lap_times	silver
6	21		21	6	race_date_raw	varchar			true		fct_lap_times	silver
7	21		21	7	lap_number	int32			true		fct_lap_times	silver
8	21		21	8	driver_id	varchar			true		fct_lap_times	silver
9	21		21	9	position_on_lap	int32			true		fct_lap_times	silver
10	21		21	10	lap_time_raw	varchar			true		fct_lap_times	silver
1	22		22	1	season_year	int32			true		fct_pit_stops	silver
2	22		22	2	round	int32			true		fct_pit_stops	silver
3	22		22	3	race_name	varchar			true		fct_pit_stops	silver
4	22		22	4	circuit_id	varchar			true		fct_pit_stops	silver
5	22		22	5	circuit_name	varchar			true		fct_pit_stops	silver
6	22		22	6	race_date_raw	varchar			true		fct_pit_stops	silver
7	22		22	7	race_time_raw	varchar			true		fct_pit_stops	silver
8	22		22	8	driver_id	varchar			true		fct_pit_stops	silver
9	22		22	9	lap_number	int32			true		fct_pit_stops	silver
10	22		22	10	stop_number	int32			true		fct_pit_stops	silver
11	22		22	11	pit_time_of_day_raw	varchar			true		fct_pit_stops	silver
12	22		22	12	stop_duration_raw	varchar			true		fct_pit_stops	silver
1	23		23	1	season_year	int32			true		fct_qualifying	silver
2	23		23	2	round	int32			true		fct_qualifying	silver
3	23		23	3	race_name	varchar			true		fct_qualifying	silver
4	23		23	4	circuit_id	varchar			true		fct_qualifying	silver
5	23		23	5	circuit_name	varchar			true		fct_qualifying	silver
6	23		23	6	race_date_raw	varchar			true		fct_qualifying	silver
7	23		23	7	driver_id	varchar			true		fct_qualifying	silver
8	23		23	8	driver_code	varchar			true		fct_qualifying	silver
9	23		23	9	given_name	varchar			true		fct_qualifying	silver
10	23		23	10	family_name	varchar			true		fct_qualifying	silver
11	23		23	11	constructor_name	varchar			true		fct_qualifying	silver
12	23		23	12	constructor_id	varchar			true		fct_qualifying	silver
13	23		23	13	quali_position	int32			true		fct_qualifying	silver
14	23		23	14	q1_time_raw	varchar			true		fct_qualifying	silver
15	23		23	15	q2_time_raw	varchar			true		fct_qualifying	silver
16	23		23	16	q3_time_raw	varchar			true		fct_qualifying	silver
1	24		24	1	season_year	int32			true		dim_races	silver
2	24		24	2	round	int32			true		dim_races	silver
3	24		24	3	race_name	varchar			true		dim_races	silver
4	24		24	4	circuit_id	varchar			true		dim_races	silver
5	24		24	5	circuit_name	varchar			true		dim_races	silver
6	24		24	6	latitude	float64			true		dim_races	silver
7	24		24	7	longitude	float64			true		dim_races	silver
8	24		24	8	city	varchar			true		dim_races	silver
9	24		24	9	country	varchar			true		dim_races	silver
10	24		24	10	race_date_raw	varchar			true		dim_races	silver
11	24		24	11	race_time_raw	varchar			true		dim_races	silver
12	24		24	12	fp1_date_raw	varchar			true		dim_races	silver
13	24		24	13	fp1_time_raw	varchar			true		dim_races	silver
14	24		24	14	fp2_date_raw	varchar			true		dim_races	silver
15	24		24	15	fp2_time_raw	varchar			true		dim_races	silver
16	24		24	16	fp3_date_raw	varchar			true		dim_races	silver
17	24		24	17	fp3_time_raw	varchar			true		dim_races	silver
18	24		24	18	qualifying_date_raw	varchar			true		dim_races	silver
19	24		24	19	qualifying_time_raw	varchar			true		dim_races	silver
20	24		24	20	sprint_date_raw	varchar			true		dim_races	silver
21	24		24	21	sprint_time_raw	varchar			true		dim_races	silver
1	25		25	1	season_year	int32			true		fct_results	silver
2	25		25	2	round	int32			true		fct_results	silver
3	25		25	3	race_name	varchar			true		fct_results	silver
4	25		25	4	circuit_id	varchar			true		fct_results	silver
5	25		25	5	circuit_name	varchar			true		fct_results	silver
6	25		25	6	race_date_raw	varchar			true		fct_results	silver
7	25		25	7	time_raw	varchar			true		fct_results	silver
8	25		25	8	driver_id	varchar			true		fct_results	silver
9	25		25	9	driver_name	varchar			true		fct_results	silver
10	25		25	10	constructor_id	varchar			true		fct_results	silver
11	25		25	11	constructor_name	varchar			true		fct_results	silver
12	25		25	12	grid_position	int32			true		fct_results	silver
13	25		25	13	finish_position	int32			true		fct_results	silver
14	25		25	14	points	float64			true		fct_results	silver
15	25		25	15	laps_completed	int32			true		fct_results	silver
16	25		25	16	status	varchar			true		fct_results	silver
17	25		25	17	result_time_raw	varchar			true		fct_results	silver
18	25		25	18	fastest_lap_number	int32			true		fct_results	silver
19	25		25	19	fastest_lap_time_raw	varchar			true		fct_results	silver
20	25		25	20	average_speed_raw	varchar			true		fct_results	silver
1	26		26	1	season	varchar			true		silver_results	silver
2	26		26	2	round	int32			true		silver_results	silver
3	26		26	3	raceName	varchar			true		silver_results	silver
4	26		26	4	circuitId	varchar			true		silver_results	silver
5	26		26	5	circuitName	varchar			true		silver_results	silver
6	26		26	6	date	date			true		silver_results	silver
7	26		26	7	time	varchar			true		silver_results	silver
8	26		26	8	driverId	varchar			true		silver_results	silver
9	26		26	9	driverName	varchar			true		silver_results	silver
10	26		26	10	constructorId	varchar			true		silver_results	silver
11	26		26	11	constructorName	varchar			true		silver_results	silver
12	26		26	12	grid	int32			true		silver_results	silver
13	26		26	13	position	int32			true		silver_results	silver
14	26		26	14	points	int32			true		silver_results	silver
15	26		26	15	laps	int32			true		silver_results	silver
16	26		26	16	status	varchar			true		silver_results	silver
17	26		26	17	fastestLap	varchar			true		silver_results	silver
18	26		26	18	fastestLapTime	varchar			true		silver_results	silver
19	26		26	19	averageSpeed	decimal(18,3)			true		silver_results	silver
1	27		27	1	1	int32			true		fct_sprint_results	silver
1	28		28	1	season_year	int32			true		driver_season_race_results	gold
2	28		28	2	round	int32			true		driver_season_race_results	gold
3	28		28	3	race_name	varchar			true		driver_season_race_results	gold
4	28		28	4	circuit_id	varchar			true		driver_season_race_results	gold
5	28		28	5	circuit_name	varchar			true		driver_season_race_results	gold
6	28		28	6	driver_id	varchar			true		driver_season_race_results	gold
7	28		28	7	constructor_id	varchar			true		driver_season_race_results	gold
8	28		28	8	grid_position	int32			true		driver_season_race_results	gold
9	28		28	9	finish_position	int32			true		driver_season_race_results	gold
10	28		28	10	points	float64			true		driver_season_race_results	gold
11	28		28	11	laps_completed	int32			true		driver_season_race_results	gold
12	28		28	12	status	varchar			true		driver_season_race_results	gold
13	28		28	13	positions_gained	int32			true		driver_season_race_results	gold
14	28		28	14	is_podium	int32			true		driver_season_race_results	gold
15	28		28	15	is_dnf	int32			true		driver_season_race_results	gold
    """
)

user_question = (
    "what was lewis hamiltons quickest quali lap in 2024 for q1, q2  and Q3 and where was it? - Dont cast the results"
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


# 1. Connect
con = duckdb.connect(":memory:")

# 2. Load extension
con.execute("""ATTACH 'ducklake:F1metadata.ducklake' AS datalake
    (DATA_PATH '/Users/joshuaward/Documents/Data-Engineering/F1Project/F1Project/f1_duckdb_project/data/');""")

con.execute("use datalake;")


df = con.execute(sql_command).fetch_df()
records = df.to_dict(orient="records")
json_str = json.dumps(records, indent=2)

print(json_str)


system_instructions_question = ("The user has asked the below question. Which has the prompted the response in JSON format. Create a summary using the question and the response that is JSON format. Write it in a formula 1 commentator format ")

response = client.models.generate_content(
    model="gemini-2.5-flash",  # or "gemini-2.5-pro" for heavier reasoning
    contents=[
        {"role": "user", "parts": [
            {"text": system_instructions_question},
            {"text": user_question},
            {"text": json_str}
        ]}
    ]
)

summary = response.text
print(summary)