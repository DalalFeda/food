import requests
import pprint as pp
import pandas as pd
import sqlalchemy as sql
import configparser as cfg
import time
import json

config = cfg.ConfigParser()
config.read('food.cfg')

USER = config['sqlserver']['user']
PASSWORD = config['sqlserver']['password']
SERVER = config['sqlserver']['server']
DATABASE = config['sqlserver']['database']
DRIVER = config['sqlserver']['driver']
connection_string = (f"mssql+pyodbc://{USER}:{PASSWORD}@{SERVER}/"
                     f"{DATABASE}?driver={DRIVER}")
sql_engine = sql.create_engine(connection_string)

api_key = config['food']['api_key']


def load_to_database(dataframe, table_name):
    dataframe.to_sql(table_name, sql_engine, if_exists="replace", index=False)


def download_food_records() -> list:
    food_records = list()
    page_number = 1

    while True:
        print(f"getting page {page_number}")
        url = (
            f"https://api.nal.usda.gov/fdc/v1/foods/list"
            f"?pageNumber={page_number}&api_key={api_key}"
        )
        response = requests.get(url)
        json = response.json()

        if isinstance(json, dict):
            print(json)
            break

        food_records += json

        if len(json) == 50:
            page_number += 1
            time.sleep(0.2)
            continue

        break

    return list(food_records)


def process_food_records(food_records: list):

    food_df = pd.DataFrame(food_records)

    # food_type = json
    # print(type(food_type))
    # for record in json:
    #     pp.pprint(record)
    #     break
    # print(food_df.columns)
    # reset_index -> makes index into a column
    food_columns = [
        "fdcId",
        "description",
        "dataType",
        "publicationDate",
        "foodCode",
        "ndbNumber",
    ]

    print("processing food nutrients")
    food_nutrients = food_df["foodNutrients"]
    food_nutrients = pd.DataFrame(
        food_nutrients.explode().apply(pd.Series)
    ).reset_index()
    food_nutrients = food_nutrients.rename(columns={"index": "food_index"})

    food_df = food_df[food_columns].reset_index()

    print(f"loading food ({len(food_df)})")
    load_to_database(food_df, "food")

    print(f"loading food nutrients ({len(food_nutrients)} records)")
    load_to_database(food_nutrients, "food_nutrients")


if __name__ == "__main__":

    try:
        # read json file "food.json"
        with open("food.json", "r") as food_json_file:
            food_records = json.load(food_json_file)
    except FileNotFoundError:

        # if "food.json" doesn't exist,
        # download the food records
        food_records = download_food_records()

        # and save it to a json file
        with open("food.json", "w") as food_json_file:
            json.dump(food_records, food_json_file)

    process_food_records(food_records)
