#DÖVİZ KURLARINI TCMB'DEN ÇEKMEK VE SQL SERVER VERİTABANINA KAYDETMEK

import requests
import pyodbc
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta

# TCMB XML döviz kurları URL'si
BASE_URL = "https://www.tcmb.gov.tr/kurlar/"

def get_exchange_rates(date):
    url = f"{BASE_URL}{date.year}{date.month:02d}/{date.day:02d}{date.month:02d}{date.year}.xml"
    response = requests.get(url)
    response.raise_for_status()  # Hata varsa fırlatır
    return response.content

def parse_exchange_rates(xml_data):
    root = ET.fromstring(xml_data)
    date_str = root.attrib["Tarih"]
    date = datetime.strptime(date_str, '%d.%m.%Y').strftime('%Y-%m-%d')  # Tarihi doğru formata dönüştür
    for currency in root.findall("Currency"):
        currency_code = currency.attrib["CurrencyCode"]
        if currency_code == "EUR":
            forex_buying = currency.find("ForexBuying").text
            forex_selling = currency.find("ForexSelling").text
            return (date, currency_code, forex_buying, forex_selling)
    return None

def insert_exchange_rates(rates):
    conn = pyodbc.connect('DRIVER={SQL Server};SERVER=10.16.210.28;DATABASE=DB;UID=USR;PWD=PASS')
    cursor = conn.cursor()

    for rate in rates:
        cursor.execute('''
            INSERT INTO ExchangeRates (Date, CurrencyCode, ForexBuying, ForexSelling)
            VALUES (?, ?, ?, ?)
        ''', rate)

    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    start_date = datetime(2021, 1, 1)
    end_date = datetime(2021, 12, 31)
    delta = timedelta(days=1)

    rates = []
    while start_date <= end_date:
        try:
            xml_data = get_exchange_rates(start_date)
            rate = parse_exchange_rates(xml_data)
            if rate:
                rates.append(rate)
        except Exception as e:
            print(f"Failed to retrieve data for {start_date.strftime('%Y-%m-%d')}: {e}")
        start_date += delta

    if rates:
        insert_exchange_rates(rates)