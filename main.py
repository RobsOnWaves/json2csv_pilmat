# This is a sample Python script.
import pandas as pd
from pathlib import Path
import json
import pytz
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS

# Press Maj+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    directory = Path("data")
    files = list(directory.glob("*.txt"))

    # Get a list of files in the directory
    for file in files:

        with open(file, 'r') as f:
            data = json.load(f)
            df = pd.json_normalize(data, record_path=file.name.split(' ')[0])
            df['horodatage'] = pd.to_datetime(df['horodatage'], unit='s', utc=True)
            df_for_influx = df
            df['horodatage'] = df.apply(lambda row: row['horodatage'].astimezone(pytz.timezone('Europe/Paris')).strftime(
                '%Y-%m-%d %H:%M:%S %Z%z'), axis=1)
            df_for_influx = df_for_influx.set_index('horodatage')
            df = df.set_index('horodatage')

            # Write data to InfluxDB
            with InfluxDBClient(url="http://localhost:8086", token="token_influx", org="your-organisation") as client:
                client.write_api(write_options=SYNCHRONOUS).write(bucket='your-bucket', record=df_for_influx,
                                                                  data_frame_measurement_name=file.name.split(' ')[0])

        if 'e1' in file.name or 'e2' in file.name:
            csv = df.to_csv(file.name.split(' ')[0] + "_" + file.name.split(' ')[1] + "_CET.csv", sep=';')
        elif 'cnx':
            csv = df.to_csv(file.name.split(' ')[0] + "_" + 'cnx' + "_CET.csv", sep=';')
        elif 'evt':
            csv = df.to_csv(file.name.split(' ')[0] + "_" + 'evt' + "_CET.csv", sep=';')
        else:
            print('not handled file')



# See PyCharm help t https://www.jetbrains.com/help/pycharm/
