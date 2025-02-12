import json
import requests
import csv
import re
import sys
import os 
from io import StringIO 
from google.cloud import firestore  
from google.oauth2 import service_account
from datetime import datetime

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

def save_to_firestore(json_data):
    """
    Saves the JSON data to a Firestore database.
    :param json_data: The structured data to save.
    """
    try:
        credentials_json = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_CONTENT")
        if not credentials_json:
            raise ValueError("GOOGLE_APPLICATION_CREDENTIALS_CONTENT environment variable is not set.")
        
        credentials = service_account.Credentials.from_service_account_info(json.loads(credentials_json))
        db = firestore.Client(credentials=credentials)
        # Get the current date in YYYY-MM-DD format
        current_date = datetime.now().strftime("%Y-%m-%d")
        doc_ref = db.collection("fuel_data").document(current_date)  
        doc_ref.set(json_data) 
        print("Data has been successfully saved to Firestore.")
    except Exception as e:
        print(f"Error while saving to Firestore: {e}")
        sys.exit(1)  

def run_task():
    """
    Main task runner function to download data and prepare it for further processing.
    """
    
    csv_url = os.getenv("CSV_URL", "https://www.mimit.gov.it/images/stories/carburanti/MediaRegionaleStradale.csv")  
    data = download(csv_url)  
    json_data = data_preparation(data[1:])  
    save_to_firestore(json_data) 

if __name__ == "__main__":
    run_task()
