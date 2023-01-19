# This is a sample Python script.
import pandas as pd
from pathlib import Path
import json

# Press Maj+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    directory = Path("data")
    files = list(directory.glob("*.txt"))

    # Get a list of files in the directory
    for file in files:
        if 'cnx' not in file.name:
            with open(file, 'r') as f:
                data = json.load(f)
                df = pd.json_normalize(data, record_path=file.name.split(' ')[0])
                df['horodatage'] = pd.to_datetime(df['horodatage'], unit='s')
                df = df.set_index('horodatage')
            csv = df.to_csv(file.name.split(' ')[0] + "_" + file.name.split(' ')[1] + ".csv")

    # Read the JSON file



# See PyCharm help t https://www.jetbrains.com/help/pycharm/
