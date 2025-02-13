import json
import requests
import csv
import re
import sys
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from io import StringIO
from google.cloud import firestore
from google.oauth2 import service_account
from datetime import datetime, timedelta
from statsmodels.tsa.arima.model import ARIMA

def download(csv_url):
    """
    Downloads a CSV file from the specified URL, decodes its content, and reads it into a list of dictionaries.
    :param csv_url: The URL to download the CSV file from.
    :return: A list of dictionaries representing the rows of the CSV file.
    """
    try:
        response = requests.get(csv_url)
        response.raise_for_status()
        csv_string = response.content.decode('utf-8')
        csv_file = StringIO(csv_string)
        reader = csv.DictReader(csv_file)
        data = [row for row in reader]
        print(f"Data has been downloaded successfully:\n{data}")
        return data
    except requests.exceptions.RequestException as e:
        print(f"Error while downloading file: {e}")
        sys.exit(1)

def data_preparation(data):
    """
    Processes the downloaded data to prepare a JSON-like nested dictionary structure.
    :param data: A list of dictionaries representing CSV rows.
    :return: A nested dictionary containing structured data.
    """
    json_data = {}
    pattern = r"([^;]+);([^;]+);([^;]+);([^;]+)"

    for item in data:
        value_str = "".join(item.values())
        date_str = "".join(item.keys()).split(" ")[1]

        match = re.match(pattern, value_str)

        if match:
            # Extract and convert matched groups to lowercase
            regione, carburante, servizio, prezzo = (value.lower() for value in match.groups())

            # Build the nested dictionary structure
            if regione not in json_data:
                json_data[regione] = {}
            if carburante not in json_data[regione]:
                json_data[regione][carburante] = {}
            if servizio not in json_data[regione][carburante]:
                json_data[regione][carburante]['price'] = prezzo
                json_data[regione][carburante]['type'] = servizio
        else:
            print(f"No match found during data preparation for: {value_str}")
            sys.exit(1)

        json_data['date'] = date_str

    return json_data

def save_on_firestore(json_data, collection_name, db):
    """
    Saves the JSON data to a Firestore database.
    :param json_data: The structured data to save.
    """
    try:
        # Get the current date in YYYY-MM-DD format
        current_date = datetime.now().strftime("%Y-%m-%d")
        doc_ref = db.collection(collection_name).document(current_date)
        doc_ref.set(json_data)
        print(f"Data has been successfully saved to Firestore collection: {collection_name}")
    except Exception as e:
        print(f"Error while saving to Firestore: {e}")
        sys.exit(1)

def get_from_firestore(num_of_entries, collection_name, db):
    """
    Retrieves documents from the Firestore database collection.
    """
    try:
        entries = db.collection(collection_name).order_by("date", direction=firestore.Query.DESCENDING).limit(num_of_entries).stream()
        docs = [doc.to_dict() for doc in entries]
        print(f"Last {num_of_entries} entries retrieved successfully.")
        return docs
    except Exception as e:
        print(f"Error while retrieving data on Firestore: {e}")
        sys.exit(1)

def data_preparation_for_forecast(docs):
  region2prices_gasolio = {
      regione: [float(item[regione]['gasolio']['price']) for item in docs if regione in item and 'gasolio' in item[regione]]
      for regione in {reg for obj in docs for reg in obj.keys() if isinstance(obj[reg], dict)}
  }

  region2prices_gpl = {
      regione: [float(item[regione]['gpl']['price']) for item in docs if regione in item and 'gpl' in item[regione]]
      for regione in {reg for obj in docs for reg in obj.keys() if isinstance(obj[reg], dict)}
  }

  region2prices_benzina = {
      regione: [float(item[regione]['benzina']['price']) for item in docs if regione in item and 'benzina' in item[regione]]
      for regione in {reg for obj in docs for reg in obj.keys() if isinstance(obj[reg], dict)}
  }

  region2prices_metano = {
      regione: [float(item[regione]['metano']['price']) for item in docs if regione in item and 'metano' in item[regione]]
      for regione in {reg for obj in docs for reg in obj.keys() if isinstance(obj[reg], dict)}
  }

  region2prices = {
      "gasolio": region2prices_gasolio,
      "gpl": region2prices_gpl,
      "benzina": region2prices_benzina,
      "metano": region2prices_metano
  }

  return region2prices

def forecast(prices):
  if len(prices) == 0:
    return -1

  if len(prices) < 5:
    return round(sum(prices) / len(prices), 2)

  try:
    print("Running ARIMA model for price forecast")
    series = pd.Series(prices)
    modello = ARIMA(series, order=(1, 1, 1))
    modello_fittato = modello.fit()
    previsione = modello_fittato.forecast(steps=1)
    previsione_float = round(previsione.iloc[0], 2)
    return previsione_float
  except Exception as e:
    print(f"Error while forecasting: {e}")
    sys.exit(1)

def run_task():
    """
    Main task runner function to download data and prepare it for further processing.
    """
    
	# Setup ENV variables
	csv_url = os.getenv("CSV_URL", "https://www.mimit.gov.it/images/stories/carburanti/MediaRegionaleStradale.csv")
    num_of_entries = os.getenv("FIRESTORE_FORECAST_WINDOW", "30")
    collection_name = os.getenv("FIRESTORE_COLLECTION_NAME", "fuel_data")
    collection_name_forecast = os.getenv("FIRESTORE_COLLECTION_NAME_FORECAST", "fuel_data_forecast")
    forecast_is_enabled = os.getenv("FORECAST_IS_ENABLED", "true")
    credentials_json = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_CONTENT")
    if not credentials_json:
      raise ValueError("GOOGLE_APPLICATION_CREDENTIALS_CONTENT environment variable is not set.")

    credentials = service_account.Credentials.from_service_account_info(json.loads(credentials_json))
    db = firestore.Client(credentials=credentials)
	
	# Retrieves data
    data = download(csv_url)
    json_data = data_preparation(data[1:])
    save_on_firestore(json_data, collection_name, db)
    

    # Forecast
    if forecast_is_enabled == "true":
      print("Running forecast functions.")
      current_date = datetime.now().strftime("%Y-%m-%d")
      tomorrow_date = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")

      docs = get_from_firestore(int(num_of_entries), collection_name, db)
      prices_forecast = data_preparation_for_forecast(docs)

      for type_of_fuel in prices_forecast:
        for region, prices in prices_forecast[type_of_fuel].items():
            prices_forecast[type_of_fuel][region] = {
                "prices": prices,
                "forecast": forecast(prices)
            }
      
      prices_forecast["current_date"] = current_date
      prices_forecast["tomorrow_date"] = tomorrow_date

      save_on_firestore(prices_forecast, collection_name_forecast, db)
    else:
      print("Forecast disabled.")

if __name__ == "__main__":
    start_time = datetime.now()
    print(f"Start: {start_time}")

    run_task()

    end_time = datetime.now()
    print(f"End: {end_time}")

    elapsed_time = end_time - start_time
    print(f"Elapsed time: {elapsed_time}")
