from google import genai
import json
import os


# The client gets the API key from the environment variable `GEMINI_API_KEY`.
client = genai.Client()

# 2. Example F1 race data (you would build this from your pipeline)
race_summary_data = {
    "race": "2024 Japanese Grand Prix",
    "circuit": "Suzuka",
    "weather": "Dry, sunny",
    "safety_cars": 1,
    "drivers": [
        {
            "driver": "Max Verstappen",
            "team": "Red Bull",
            "position": 1,
            "start_position": 2,
            "pit_stops": 2,
            "total_time_seconds": 5503.12,
            "stint_pace": [92.1, 92.4, 92.8],  # example avg lap times per stint
        },
        {
            "driver": "Lando Norris",
            "team": "McLaren",
            "position": 2,
            "start_position": 1,
            "pit_stops": 2,
            "total_time_seconds": 5507.80,
            "stint_pace": [92.3, 92.6, 93.0],
        },
        # ...
    ],
    "events": [
        {"lap": 1, "type": "crash", "description": "Verstappen passes Norris into Turn 1 and OMG they crash!"},
        {"lap": 24, "type": "pit_stop", "description": "Leaders pit for hard tyres"},
        {"lap": 32, "type": "safety_car", "description": "Safety car for debris at Degner"},
    ],
}

# 3. Build your prompt
system_instructions = (
    "You are an expert Formula 1 race analyst. "
    "Given structured race data, explain what happened in clear, engaging language. "
    "Highlight strategy, pace differences, key overtakes, and turning points. "
    "Avoid fabricating details that are not supported by the data."
)

user_prompt = (
    "Please summarise this race for a casual F1 fan. "
    "Explain who was strong, where the race was decided, and any interesting patterns. "
    "Keep it under 400 words."
)

# 4. Call Gemini 2.5 with text + JSON
response = client.models.generate_content(
    model="gemini-2.5-flash",  # or "gemini-2.5-pro" for heavier reasoning
    contents=[
        {"role": "user", "parts": [
            {"text": system_instructions},
            {"text": user_prompt},
            {"text": "Here is the race data in JSON format:"},
            {"text": json.dumps(race_summary_data)}
        ]}
    ]
)

# 5. Extract the summary text
summary = response.text
print(summary)