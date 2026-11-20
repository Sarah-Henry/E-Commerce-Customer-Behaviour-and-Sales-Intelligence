import requests
import pandas as pd
import json
from pathlib import Path

# Create output folder
output_folder = Path("data/external_api")
output_folder.mkdir(parents=True, exist_ok=True)

# -----------------------------------------
# API 1: OPEN-METEO HISTORICAL WEATHER DATA
# -----------------------------------------

print("\nDownloading historical weather data...")

weather_url = "https://archive-api.open-meteo.com/v1/archive"

weather_params = {
    "latitude": -23.55,
    "longitude": -46.63,
    "start_date": "2016-01-01",
    "end_date": "2018-12-31",
    "daily": "temperature_2m_mean,precipitation_sum",
    "timezone": "America/Sao_Paulo"
}

try:
    response = requests.get(
        weather_url,
        params=weather_params,
        timeout=60
    )
    response.raise_for_status()

    weather_data = response.json()

    # Save original API response as JSON
    with open(output_folder / "weather_raw.json", "w") as file:
        json.dump(weather_data, file, indent=4)

    # Convert daily weather data into a CSV
    weather_df = pd.DataFrame(weather_data["daily"])
    weather_df.rename(columns={"time": "date"}, inplace=True)

    weather_df.to_csv(
        output_folder / "weather_data.csv",
        index=False
    )

    print("Weather data downloaded successfully!")
    print("Weather records:", len(weather_df))
    print(weather_df.head())

except Exception as error:
    print("Weather API error:", error)


# -----------------------------------------
# API 2: NAGER.DATE BRAZIL PUBLIC HOLIDAYS
# -----------------------------------------

print("\nDownloading Brazilian public holidays...")

all_holidays = []

for year in [2016, 2017, 2018]:

    holiday_url = (
        f"https://date.nager.at/api/v3/"
        f"PublicHolidays/{year}/BR"
    )

    try:
        response = requests.get(holiday_url, timeout=30)
        response.raise_for_status()

        holiday_data = response.json()
        all_holidays.extend(holiday_data)

        print(f"{year} holidays downloaded!")

    except Exception as error:
        print(f"Holiday API error for {year}:", error)

if all_holidays:
    # Save original API response
    with open(output_folder / "holidays_raw.json", "w") as file:
        json.dump(all_holidays, file, indent=4)

    # Convert holiday data into a CSV
    holidays_df = pd.DataFrame(all_holidays)

    selected_columns = [
        "date",
        "localName",
        "name",
        "countryCode",
        "global"
    ]

    holidays_df = holidays_df[selected_columns]

    holidays_df.to_csv(
        output_folder / "brazil_holidays.csv",
        index=False
    )

    print("\nHoliday data downloaded successfully!")
    print("Holiday records:", len(holidays_df))
    print(holidays_df.head())

else:
    print("No holiday data was downloaded.")

print("\n===================================")
print("EXTERNAL API EXTRACTION COMPLETED")
print("===================================")
