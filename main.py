# This is a sample Python script.
import pandas as pd
import datetime
from pathlib import Path
import json
import pytz
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS

# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    directory = Path("data")
    files = list(directory.glob("*.txt"))
    dts_ids = []
    error_on_influx = False
    fix_sirse_date = False
    offset_timestamp = 3600 if fix_sirse_date else 0
    date_of_insertion = datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
    # Get a list of files in the directory


    for file in files:
        if file.name.split(' ')[0] not  in dts_ids:
            dts_ids.append(file.name.split(' ')[0])
        with open(file, 'r') as f:
            data = json.load(f)
            df = pd.json_normalize(data, record_path=file.name.split(' ')[0])
            df_for_influx = pd.json_normalize(data, record_path=file.name.split(' ')[0])
            df['horodatage'] = df['horodatage'] + offset_timestamp
            df_for_influx['horodatage'] = df_for_influx['horodatage'] + offset_timestamp
            df['horodatage'] = pd.to_datetime(df['horodatage'], unit='s', utc=True)
            df_for_influx['horodatage'] = pd.to_datetime(df_for_influx['horodatage'], unit='s', utc=True)
            df_for_influx['date_of_insertion'] = date_of_insertion
            df['horodatage'] = df.apply(lambda row: row['horodatage'].astimezone(pytz.timezone('Europe/Paris')).strftime(
                '%Y-%m-%d %H:%M:%S %Z%z'), axis=1)
            df = df.set_index('horodatage')
            df_for_influx = df_for_influx.set_index('horodatage')

            try:
                # Write data to InfluxDB
                with InfluxDBClient(url="http://localhost:8086", token="token_influx", org="your-organisation") as client:

                    client.write_api(write_options=SYNCHRONOUS).write(  bucket='your-bucket', record=df_for_influx,
                                                                        data_frame_measurement_name=file.name.split(' ')[0],
                                                                        data_frame_tag_columns=['date_of_insertion'])
                #clientdf.write_points(df_for_influx, 'demo', tag_columns='date_of_insertion')
            except Exception as e:
                print('No connection to InfluxDB')
                error_on_influx = True

        if 'e1' in file.name or 'e2' in file.name:
            csv = df.to_csv(file.name.split(' ')[0] + "_" + file.name.split(' ')[1] + "_CET.csv", sep=';')
        elif 'cnx':
            csv = df.to_csv(file.name.split(' ')[0] + "_" + 'cnx' + "_CET.csv", sep=';')
        elif 'evt':
            csv = df.to_csv(file.name.split(' ')[0] + "_" + 'evt' + "_CET.csv", sep=';')
        else:
            print('not handled file')

    if not error_on_influx:
        with InfluxDBClient(url="http://localhost:8086", token="token_influx", org="your-organisation") as client:
            query_api = client.query_api()
            with pd.ExcelWriter('output.xlsx') as writer:
                for dts_id in dts_ids:
                    query = 'from(bucket: "your-bucket")\
                            |> range(start: 2020-01-01, stop: 2023-02-01)\
                            |> filter(fn: (r) => r["_measurement"] == "' + dts_id + '" and r.date_of_insertion == "' + date_of_insertion + '")\
                            |> filter(fn: (r) => r["_field"] == "e1" or r["_field"] == "e2")\
                            |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")'

                    data_frame = query_api.query_data_frame(query=query)
                    data_frame = data_frame.drop(columns=['_start', '_stop', 'result', 'table', '_measurement'])
                    data_frame['_time'] = data_frame.apply(
                        lambda row: row['_time'].astimezone(pytz.timezone('Europe/Paris')).strftime(
                        '%Y-%m-%d %H:%M:%S %Z%z'), axis=1)
                    data_frame['_time'] = data_frame['_time'].apply(lambda a: str(a))
                    data_frame['date'] = data_frame['_time'].apply(lambda a: a.split(" ")[0])
                    data_frame['heure'] = data_frame['_time'].apply(lambda a: a.split(" ")[1].split('+')[0])
                    data_frame = data_frame.reindex(columns=['date', 'heure', 'e1', 'e2', '_time'])
                    data_frame.to_excel(writer, sheet_name=dts_id)

    # See PyCharm help t https://www.jetbrains.com/help/pycharm/
