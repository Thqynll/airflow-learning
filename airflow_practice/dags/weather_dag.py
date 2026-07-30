import requests
from datetime import datetime, timedelta
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

US_PM25_BP = [
    {"bplow": 0.0,  "bphigh": 9.0,   "ilow": 0,   "ihigh": 50,  "label": "p2"},
    {"bplow": 9.1,  "bphigh": 35.4,  "ilow": 51,  "ihigh": 100, "label": "p2"},
    {"bplow": 35.5, "bphigh": 55.4,  "ilow": 101, "ihigh": 150, "label": "p2"},
    {"bplow": 55.5, "bphigh": 125.4, "ilow": 151, "ihigh": 200, "label": "p2"},
    {"bplow": 125.5,"bphigh": 225.4, "ilow": 201, "ihigh": 300, "label": "p2"},
    {"bplow": 225.5,"bphigh": 325.4, "ilow": 301, "ihigh": 500, "label": "p2"}
]

CN_PM25_BP = [
    {"bplow": 0,   "bphigh": 35,  "ilow": 0,   "ihigh": 50,  "label": "p2"},
    {"bplow": 35,  "bphigh": 75,  "ilow": 51,  "ihigh": 100, "label": "p2"},
    {"bplow": 75,  "bphigh": 115, "ilow": 101, "ihigh": 150, "label": "p2"},
    {"bplow": 115, "bphigh": 150, "ilow": 151, "ihigh": 200, "label": "p2"},
    {"bplow": 150, "bphigh": 250, "ilow": 201, "ihigh": 300, "label": "p2"},
    {"bplow": 250, "bphigh": 500, "ilow": 301, "ihigh": 500, "label": "p2"}
]

def create_schema():
    hook = PostgresHook(postgres_conn_id="postgres_aqi")
    with open("/opt/airflow/air.sql") as f:
        sql = f.read()
    hook.run(sql)

def calculate_aqi(conc, breakpoints):
    for bp in breakpoints:
        if bp["bplow"] <= conc <= bp["bphigh"]:
            aqi = ((bp["ihigh"] - bp["ilow"]) / (bp["bphigh"] - bp["bplow"])) * (conc - bp["bplow"]) + bp["ilow"]
            return round(aqi), bp["label"]
    return 0, "unknown"


# --- Main Core Logic Task ---
# NOTE: schema is no longer created here. Postgres creates city/pollution
# tables automatically on first container start via air.sql mounted into
# /docker-entrypoint-initdb.d/. This task now only reads/writes data.
def execution_pipeline_wrapper():
    create_schema()
    openweather_key = Variable.get("OPENWEATHER_API_KEY")
    hook = PostgresHook(postgres_conn_id="postgres_aqi")

    global_cities = ["Hanoi", "Hue", "Tokyo", "Paris", "New York,NY,US", "London"]
    print(f"Starting API extraction stream at {datetime.now()}")

    for target in global_cities:
        print(f"Processing target: {target}")

        weather_url = f"https://api.openweathermap.org/data/2.5/weather?q={target}&units=metric&appid={openweather_key}"
        w_response = requests.get(weather_url)
        w_res = w_response.json()

        if w_res.get("cod") != 200:
            print(f"Error: Weather service skipped location: {target}")
            continue

        lat = w_res["coord"]["lat"]
        lon = w_res["coord"]["lon"]
        city_name = w_res.get("name", target)
        country_name = w_res.get("sys", {}).get("country", "Unknown")
        state_name = country_name

        pollution_url = f"https://api.openweathermap.org/data/2.5/air_pollution?lat={lat}&lon={lon}&appid={openweather_key}"
        p_res = requests.get(pollution_url).json()
        pm25 = p_res["list"][0]["components"]["pm2_5"]

        aqi_us, main_us = calculate_aqi(pm25, US_PM25_BP)
        aqi_cn, main_cn = calculate_aqi(pm25, CN_PM25_BP)

        ts_string = datetime.fromtimestamp(p_res["list"][0]["dt"]).strftime('%Y-%m-%dT%H:%M:%SZ')
        sql_datetime = datetime.strptime(ts_string, '%Y-%m-%dT%H:%M:%SZ')

        conn = hook.get_conn()
        cursor = conn.cursor()

        try:
            city_query = """
                INSERT INTO city (cityName, stateName, countryName, latitude, longtitude)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (cityName, countryName)
                DO UPDATE SET latitude = EXCLUDED.latitude, longtitude = EXCLUDED.longtitude;
            """
            cursor.execute(city_query, (city_name, state_name, country_name, lat, lon))

            cursor.execute("SELECT cityId FROM city WHERE cityName = %s AND countryName = %s;", (city_name, country_name))
            city_id = cursor.fetchone()[0]

            pollution_query = """
                INSERT INTO pollution (cityId, datetime, ts, aqius, mainus, aqicn, maincn)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (cityId, datetime)
                DO UPDATE SET aqius = EXCLUDED.aqius, aqicn = EXCLUDED.aqicn, ts = EXCLUDED.ts;
            """
            cursor.execute(pollution_query, (city_id, sql_datetime, ts_string, aqi_us, main_us, aqi_cn, main_cn))

            conn.commit()
            print(f"Success: Pipeline synchronized '{city_name} ({country_name})'.")

        except Exception as database_error:
            print(f"Database Task Exception on target '{target}': {database_error}")
            conn.rollback()
        finally:
            cursor.close()
            conn.close()


# --- Airflow Orchestration Configurations ---
default_args = {
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='openweather_aqi_pipeline',
    default_args=default_args,
    schedule='@daily',
    start_date=datetime(2026, 7, 1),
    catchup=False
) as dag:

    task1 = PythonOperator(
        task_id='fetch_and_save_aqi_data',
        python_callable=execution_pipeline_wrapper
    )

    task1