#!/usr/bin/env python
# coding: utf-8

# In[349]:


import pandas as pd
import numpy as np
import json 
import time 
import requests
import sys 
import os
from datetime import datetime


# In[350]:


# API-Daten als Variablen. 
# Nachher über os.getenv(), vor allem für die Cloud

# Über OS
API_KEY = os.getenv("KUCOIN_API_KEY")
API_SECRET = os.getenv("KUCOIN_API_SECRET")
API_PASSPHRASE = os.getenv("KUCOIN_API_PASSPHRASE")


# In[ ]:





# In[ ]:





# In[ ]:


# Codeblock, um die Daten aus MongoDB ins Skript zu importieren 
# Connection zur Database und Cluster wird zudem hergestellt. 

from urllib.parse import quote_plus
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

user = os.getenv("mongo_db_username")
#print(f"Plot des Username aus Google Secret Manager env: {user}")
pw = os.getenv("mongo_db_pw")
#print(f"Plot des PW aus Google Secret Manager env: {pw}")

username = quote_plus(user) # Nachher als Variable in der Cloud
password = quote_plus(pw) # Nachher als Variable in der Cloud
cluster = "icp-testing"

uri = "mongodb+srv://" + username + ":" + password + "@" + cluster + ".rpiyhke.mongodb.net/?appName=ICP-Testing"

# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))

# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)
    
# Abruf der Daten aus Mongo

db = client["master_data_ts"]
collection = db["strategy_perf_live_ts_2"]

raw_data = list(collection.find({}, {"_id": 0}))

dict_to_df = {}

# Dictionary mit einzelnen Timestamps in DF (Master-DF) umwandeln 

for i in range(len(raw_data)):
    
    data_point = list(raw_data[i].values())
    
    dict_to_df[data_point[0]] = data_point[1]
    
master_df = pd.DataFrame.from_dict(dict_to_df, orient="index")

# Documents in DB löschen. Nach jeweiligen Codeblock und control-flow werden die aktuellsten Daten inseriert 

collection.delete_many({})

print("Master-DF erfolgreich importiert und alte Daten in DB gelöscht.")


# In[369]:


print(master_df.tail(10))


# In[370]:


# Für Datenbank MongoDB geht wohl nur .to_dict
# Daten (DF) in MongoDB speichern.
# Ein Document repräsentiert eine Reihe (bei Zeitreihen einen einzelnen Zeitpunkt)

# Als Funktion, die nach Ende des Codeblock-Flows aufgerufen wird 

# timeField = "timestamp" und metaField = "columns: timeField und metaField sind Ebenen, die in Mongo definiert werden
# Werden benötigt, um die Daten korrekt zu inserieren

# db = client["master_data_ts"] --> Anfangs definiert, wird hier nicht benötigt
# collection = db["strategy_perf_live_ts_2"] --> Anfangs definiert, wird hier nicht benötigt

def insert_df_in_db(df):
    
    df_to_dict = df.to_dict(orient='index')

    for key, value in df_to_dict.items():
        collection.insert_many([{"timestamp": key, "columns": value}])
        
    # Verbidung beenden, nachdem Daten in DB übertragen wurden
    client.close()


# In[371]:


# Class to handle api requests 
# KuCoin Signer als neu erstelltes Objekt
# Beinhaltet das header-handling sowie api_key etc.

import base64
import hashlib
import hmac
import logging
import os
import time
import uuid
from urllib.parse import urlencode, quote

class KcSigner:
    def __init__(self, api_key: str, api_secret: str, api_passphrase: str):
        self.api_key = API_KEY
        self.api_secret = API_SECRET
        self.api_passphrase = API_PASSPHRASE
        
        if api_passphrase and api_secret:
            self.api_passphrase = self.sign(api_passphrase.encode("utf-8"), api_secret.encode("utf-8"))
        
        if not all([api_key, api_secret, api_passphrase]):
            logging.warning("API token is empty. Access is restricted to public interfaces only.")
            
    def sign(self, plain: bytes, key: bytes) -> str:
        hm = hmac.new(key, plain, hashlib.sha256)
        return base64.b64encode(hm.digest()).decode()
    
    def headers(self, plain: str) -> dict:
        
        timestamp = str(int(time.time() * 1000))
        signature = self.sign((timestamp + plain).encode("utf-8"), self.api_secret.encode("utf-8"))
        
        return {"KC-API-KEY": self.api_key,
                "KC-API-PASSPHRASE": self.api_passphrase,
                "KC-API-TIMESTAMP": timestamp,
                "KC-API-SIGN": signature,
                "KC-API-KEY-VERSION": "3"}
    
    
def process_headers(signer: KcSigner, body: bytes, raw_url: str, request: requests.PreparedRequest, method: str):
    request.headers["Content-Type"] = "application/json"
    
    if method == "GET" or method == "DELETE":
        payload = method + raw_url
        
    else:

    # Create the payload by combining method, raw URL, and body
        payload = method + raw_url + body.decode()
        
    headers = signer.headers(payload)
    #print(headers)

    # Add headers to the request
    request.headers.update(headers)


# In[372]:


# Funktion zum Herunterladen der historischen Daten --> Nur Single-Point fetch --> Datum über utc.now oder time.now()
# Funktion bezieht sich auf einzelne Abschnitte
# ICPUSDTM
# First Open --> 1620741600000
def get_future_kc(symbol, start_date, end_date, timeframe):

    # Datums-Manipulation
    start = pd.Timestamp(start_date)
    end = pd.Timestamp(end_date)
    start = int(datetime.timestamp(start))
    end = int(datetime.timestamp(end))
    start = start * 1000
    end = end * 1000

    # URL der KuCoin-API für Futures
    url = f"https://api-futures.kucoin.com/api/v1/kline/query?symbol={symbol}&granularity={timeframe}&from={start}&to={end}"
    payload={}
    headers = {}

    response = requests.request("GET", url, headers=headers, data=payload)


    coin_dict = json.loads(response.text)
    coin_df = pd.DataFrame(coin_dict["data"], columns=["open_time", "open", "high", "low", "close", "tr_lots","tr_vol"])
    coin_df = coin_df.set_index("open_time")
    coin_df.index = pd.to_datetime(coin_df.index, unit="ms")
    coin_df = coin_df.drop(["high", "low", "close", "tr_lots","tr_vol"], axis = 1)
    #coin_df = coin_df.rename(columns = {"open": symbol})
    coin_df = coin_df.astype(float)
    #coin_df = coin_df.iloc[::-1]
    return coin_df

# Neuste Periode als Zeitstempel fetchen. Abrunden auf ganze Minuten immer notwendig.
now = datetime.now()

def rounder(t):
    if t.minute >= 30:
        return t.replace(second=0, microsecond=0, minute=30)
    else:
        return t.replace(second=0, microsecond=0, minute=0)
    
now = pd.Timestamp(rounder(now))

kucoin_single_fetch = get_future_kc(symbol="ICPUSDTM", start_date=now, end_date=now, timeframe="30")


#kucoin_multi_fetch = get_future_kc(symbol="ICPUSDTM", start_date="2025-09-30 14:00:00", end_date="2025-09-30 22:30:00", timeframe="30")
# Funktion, um den Master-DF im JSON-Format zu öffnen
# Im weiteren Verlauf start und ende noch dynamischer. Was ist, wenn algo längere zeit nicht läuft? 
# Master-DF mit den Spalten ["open", "signal", "filled_price", "in_position", "order_id", "tp_order_id"]
# "in_position" wird gefillt durch Positionsabfrage per Api
# signal --> place_limit --> time.sleep(60) --> api_get position --> position == True --> "in_position" == 1/-1


# Funktion zum Verbinden von Master-DF und Single-Point-DF im long format 
# Ggf. Datumsmanipulation (Zeitstempel umwandeln) 
def df_concatenation(df_1, df_2):
    return pd.concat([df_1, df_2], axis = 0)


# In[373]:


# Master DF updaten mit aktuellsten Zeitpunkt 
master_df_updated = df_concatenation(master_df, kucoin_single_fetch)
print("Master-DF updated mit neuestem Datenpunkt.")
print(master_df_updated.tail(10))


# In[143]:





# In[374]:


# Berechnungen durchführen wie rolling mean (oder Abstand zum MA, vola-adjustiert), volatility & volatility-percentile 
def calc_features(df):
    df["log_return"] = np.log(df["open"]).diff()
    df["open_std"] = df["open"].rolling(12).std()
    df["volatility"] = df["log_return"].rolling(24).std()
    df["sma_12"] = df["open"].rolling(12).mean() 
    df["vola_percentile"] = df["volatility"].rolling(24).quantile(q=0.7)
    df["upper"] = df["sma_12"] + 2*df["open_std"]
    df["lower"] = df["sma_12"] - 2*df["open_std"]

    # Nur Signale für Entrys. Exits sollen über eine Trailing Limit laufen, die beim MA sitzt
    
    if "signal" not in df.columns:
        df["signal"] = 0
              
    
    if (df["open"].iloc[-2] > df["lower"].iloc[-2]) and (df["open"].iloc[-1] < df["lower"].iloc[-1]) and (df["volatility"].iloc[-1] < df["vola_percentile"].iloc[-1]):
        df["signal"].iloc[-1] = 1
                                                       
    elif (df["open"].iloc[-2] < df["upper"].iloc[-2]) and (df["open"].iloc[-1] > df["upper"].iloc[-1]) and (df["volatility"].iloc[-1] < df["vola_percentile"].iloc[-1]):
        df["signal"].iloc[-1] = -1
                                                       
    else:
        df["signal"].iloc[-1] = 0
                                                       
    #df["signal"] = np.where((df["open"].iloc[-2] > df["lower"].iloc[-2]) & (df["open"].iloc[-1] < df["lower"].iloc[-1]), 1, 0)
    #df["signal"] = np.where((df["open"].iloc[-2] < df["upper"].iloc[-2]) & (df["open"].iloc[-1] > df["upper"].iloc[-1]), -1, 0)

    return df


# In[210]:





# In[375]:


# Aufruf der calc_features Funktion
master_df_updated = calc_features(master_df_updated)

master_df_updated


# In[274]:





# In[376]:


# Take Profit function --> Immer aufrufen, oder nachher nur immer pro Block? Erstmal hier lassen
def place_take_profit(signer: KcSigner, session: requests.Session, client_oid, symbol, side, typ, price, quantity):
    
    endpoint = "https://api-futures.kucoin.com"
    path = "/api/v1/st-orders"
    method = "POST"
    
    order_data=json.dumps({"clientOid": client_oid,
            "side": side,
            "symbol": symbol,
            "type": typ,
            "price": price,
            "leverage": 1,
            "marginMode": "CROSS",
            "qty": quantity})
    
    full_path = f"{endpoint}{path}"
    raw_url = path
    
    req = requests.Request(method=method, url=full_path, data=order_data).prepare()
    process_headers(signer, order_data.encode(), raw_url, req, method)
    
    resp = session.send(req)
    resp_obj = json.loads(resp.content)
    print(resp_obj)
    return resp_obj


# In[141]:





# In[109]:





# In[377]:


# Funktion zum prüfen, ob die gesetzte Limit-Order auch gefillt wurde. Einsatz, nachdem place_order stattfand
# Gilt für auch Stop-Loss bzw. TP-Orders
def check_for_fill(signer: KcSigner, session: requests.Session, oid):

    endpoint = "https://api-futures.kucoin.com"
    path = f"/api/v1/orders/{oid}"
    method = "GET"
    
    full_path = f"{endpoint}{path}"
    raw_url = path

    body = ""

    req = requests.Request(method=method, url=full_path)
    process_headers(signer, body, raw_url, req, method)
    
    prepped = session.prepare_request(req)
    resp = session.send(prepped)
    resp_obj = json.loads(resp.content)
    print(resp_obj)
    
    #return resp_obj
    
    is_active = resp_obj["data"]["isActive"]
    status = resp_obj["data"]["status"]
    filled_quantity = resp_obj["data"]["filledSize"]
    avg_price = resp_obj["data"]["avgDealPrice"]

    return is_active, status, filled_quantity, avg_price


# In[ ]:





# In[384]:


# Wenn in_position ungleich null wahr ist (also 1 oder -1), dann sollte geprüft werden, ob die Position noch aktiv ist. 
# Blockfolge für Positionsprüfung und Order-Platzierung startet ab hier 

if master_df_updated["in_position"].iloc[-2] != 0: # Wenn wahr, wird der Block ausgeführt, wenn falsch übersprungen und Position für neuste Periode wird auf Null gesetzt.
    
    # Funktion, um den aktuellen Zustand der Position zu erfragen
    def check_for_position(signer: KcSigner, session: requests.Session, symbol):

        endpoint = f"https://api-futures.kucoin.com"
        path = f"/api/v2/position?symbol={symbol}"
        method = "GET"

        full_path = f"{endpoint}{path}"
        raw_url = path

        body = ""

        req = requests.Request(method=method, url=full_path)
        process_headers(signer, body, raw_url, req, method)

        prepped = session.prepare_request(req)
        resp = session.send(prepped)
        resp_obj = json.loads(resp.content)
        print(resp_obj)

        is_open = str(resp_obj["data"][0]["isOpen"]) if len(resp_obj["data"]) != 0 else "False"
        
        #order_id = resp_obj["data"][0]["id"] - Ist nicht die OrderID. Nicht relevant, bleibt aber erstmal stehen
        
        # Quantity durch 100 teilen, da die Menge in LOT ausgegegebn wird. 1 Lot = 0.01 ICP
        quantity = resp_obj["data"][0]["currentQty"] / 100 if len(resp_obj["data"]) != 0 else 0

        return is_open, quantity

    if __name__ == "__main__":
        key = API_KEY
        secret = API_SECRET
        passphrase = API_PASSPHRASE
    
        session = requests.Session()
        signer = KcSigner(key, secret, passphrase)

        is_open, position_quantity = check_for_position(signer=signer, session=session, symbol="ICPUSDTM")

        # Codeblock zum Check, ob die Position noch offen ist. Falls offen, dann alte TP-Order löschen und neue setzen. 
        # Geschlossen, dann beim else-block Position auf Null setzen im Master-DF

        if is_open == "True": # and order_id_api_req == df["order_id"].iloc[-2]: # order_id kann Probleme machen. Wert fortlaufend füllen. Wird das schon getan?
            master_df_updated["in_position"].iloc[-1] = master_df_updated["in_position"].iloc[-2]
            master_df_updated["order_id"].iloc[-1] = master_df_updated["order_id"].iloc[-2]

            # Code für neue TP Order. Alte oder bestehende löschen und neu setzen oder anpassen

            def delete_old_tp(signer: KcSigner, session: requests.Session, oid):

                endpoint = "https://api-futures.kucoin.com"
                path = f"/api/v1/orders/{oid}"
                method = "DELETE"
                
                full_path = f"{endpoint}{path}"
                raw_url = path

                body = ""

                req = requests.Request(method=method, url=full_path)
                process_headers(signer, body, raw_url, req, method)

                prepped = session.prepare_request(req)
                resp = session.send(prepped)
                resp_obj = json.loads(resp.content)
                print(resp_obj)
                return resp_obj

            if __name__ == "__main__":
                key = API_KEY
                secret = API_SECRET
                passphrase = API_PASSPHRASE
    
                session = requests.Session()
                signer = KcSigner(key, secret, passphrase)

                delete_tp = delete_old_tp(signer, session, master_df_updated["tp_order_id"].iloc[-2])

                print("Alte TP-Order erfolgreich gelöscht" if delete_tp["code"] == "200000" else "TP-Order konnte nicht gelöscht werden")

                # Setzen einer neuen TP-Order. Hier nur der Call der Funktion

            if __name__ == "__main__":
                key = API_KEY
                secret = API_SECRET
                passphrase = API_PASSPHRASE

                session = requests.Session()
                signer = KcSigner(key, secret, passphrase)
                
                # Take-Profit mit if-else direkt in den Funktionsargumenten für long und short 
                take_profit_order = place_take_profit(signer, session,
                                                      client_oid="84734282939buytp" if master_df_updated["in_position"].iloc[-1] == 1 else "84734282939selltp",
                                                      symbol="ICPUSDTM", side="sell" if master_df_updated["in_position"].iloc[-1] == 1 else "buy", 
                                                      typ="limit", price=str(round(master_df_updated["sma_12"].iloc[-1], 3)), 
                                                      quantity=abs(position_quantity))

                print("Neue TP-Order erfolgreich gesetzt "if take_profit_order["code"] == "200000" else "TP-Order platzierung fehlgeschlagen")
                master_df_updated["tp_order_id"].iloc[-1] = take_profit_order["data"]["orderId"]


                insert_df_in_db(master_df_updated)
                print("Vorherige Position noch offen. TP wurde angepasst. Programm wird beendet")
                sys.exit(0)
              

         
        # Ist die Order geschlossen aufgrund von TP-Limit getriggert, dann soll die Position für die neuste Periode auf Null gesetzt werden.
        
        else:
            master_df_updated["in_position"].iloc[-1] = 0 
            active, status, filled_take_profit, filled_tp_price = check_for_fill(signer, session, master_df_updated["tp_order_id"].iloc[-2])
            master_df_updated["tp_order_id"].iloc[-1] = 0
            master_df_updated["filled_price"].iloc[-1] = filled_tp_price
            print("Letzte Position wurde geschlossen. Programm wird fortgesetzt.")

                                                   
# Wenn in der vorherigen Periode keine Position offen war, dann wird die neuste Periode mit einer Null für keine Position markiert. 
else:
    master_df_updated["in_position"].iloc[-1] = 0 
    print("Keine offene Position aus der letzten Periode. Bot wird fortgesetzt")


# In[308]:





# In[307]:





# In[136]:





# In[138]:





# In[278]:


# Order-Funktion für long und short positionen 
def place_order(signer: KcSigner, session: requests.Session, client_oid, symbol, side, typ, price, time_in_force, value):
    
    endpoint = "https://api-futures.kucoin.com"
    path = "/api/v1/orders"
    method = "POST"
    
    order_data=json.dumps({"clientOid": client_oid,
            "side": side,
            "symbol": symbol,
            "type": typ,
            "price": price,
            "timeInForce": time_in_force,
            "valueQty": value,
            "leverage": 1,
            "marginMode": "CROSS"})
    
    full_path = f"{endpoint}{path}"
    raw_url = path
    
    req = requests.Request(method=method, url=full_path, data=order_data).prepare()
    process_headers(signer, order_data.encode(), raw_url, req, method)
    
    resp = session.send(req)
    resp_obj = json.loads(resp.content)
    print(resp_obj)
    return resp_obj


# In[131]:





# In[132]:





# In[279]:


# Funktion zur extrahierung des aktuellen Mark-Prices. Letzter Preis aus dem neuesten Datenpunkt vermutlich
# nicht machbar. Hierzu müsste die Geschwindigkeit angepasst werden

def get_mark_price(signer: KcSigner, session: requests.Session, symbol):
   
    endpoint = "https://api-futures.kucoin.com"
    path = f"/api/v1/mark-price/{symbol}/current"
    method = "GET"
    
    full_path = f"{endpoint}{path}"
    raw_url = path

    body = ""

    req = requests.Request(method=method, url=full_path)
    process_headers(signer, body, raw_url, req, method)
    
    prepped = session.prepare_request(req)
    resp = session.send(prepped)
    resp_obj = json.loads(resp.content)
    print(resp_obj)
    
    #return resp_obj
    
    mark_price= str(resp_obj["data"]["value"])

    return mark_price


# In[280]:


# Order-Logik, basierend auf dem neuestem Signal und ob eine Position zum Beginn der Periode vorhanden ist. 
# Wenn die Order nach einer gewissen Zeit nicht gefillt wurde, soll diese storniert werden (cancel). --> IOC 
# Durch np.where für die Signalgenerierung werden natürlich im aktuellen code alle Bereiche mit über oder unter ma signiert. 
# Bei dieser Logik ist wahrscheinlich eine Anpassung relevant (nur den cross). 

if master_df_updated["signal"].iloc[-1] != 0 and master_df_updated["in_position"].iloc[-1] == 0:
    
    long_or_short = master_df_updated["signal"].iloc[-1] # Signal aus dem DF extrahieren. 

    if long_or_short == 1: # Basierend auf 1 oder -1 long oder short block

        if __name__ == "__main__":
            key = API_KEY
            secret = API_SECRET
            passphrase = API_PASSPHRASE
    
            session = requests.Session()
            signer = KcSigner(key, secret, passphrase)
    
            #mark_price = get_mark_price(signer, session, symbol="ICPUSDTM")
            buy_order = place_order(signer, session, client_oid="84734282939feeef", symbol="ICPUSDTM", side="buy", typ="limit", 
                                    price=get_mark_price(signer, session, symbol="ICPUSDTM"), time_in_force="IOC", value = "100")

            time.sleep(10)
            
            if buy_order["code"] == "200000":
                print("Limit Buy-Order erfolgreich gesetzt")
                oid = buy_order["data"]["orderId"]

                if __name__ == "__main__":
                    key = API_KEY
                    secret = API_SECRET
                    passphrase = API_PASSPHRASE
    
                    session = requests.Session()
                    signer = KcSigner(key, secret, passphrase)
                
                    is_active, status, filled_quantity, avg_price = check_for_fill(signer, session, oid)
                    
                    time.sleep(2)

                    if status == "done" and filled_quantity != 0:
                        print("Limit Buy-Order erfolgreich gefillt.")

                        master_df_updated["in_position"].iloc[-1] = 1
                        master_df_updated["filled_price"].iloc[-1] = avg_price
                        master_df_updated["order_id"].iloc[-1] = oid
            
                        if __name__ == "__main__":
                            key = API_KEY
                            secret = API_SECRET
                            passphrase = API_PASSPHRASE

                            session = requests.Session()
                            signer = KcSigner(key, secret, passphrase)

                            take_profit_order = place_take_profit(signer, session, 
                                                                  client_oid="84734282939buytp", 
                                                                  symbol="ICPUSDTM", side="sell", 
                                                                  typ="limit", price=str(round(master_df_updated["sma_12"].iloc[-1], 3)), 
                                                                  quantity=(filled_quantity/100))

                            time.sleep(2)

                            master_df_updated["tp_order_id"].iloc[-1] = take_profit_order["data"]["orderId"] # Ggf. noch Check, ob Limit erfolgreich gesetzt.

                            print("TP-Order für neu eröffnete Position gesetzt" if take_profit_order["code"] == "200000" else "TP-Order für neu eröffnete Position konnte nicht gesetzt werden")

                            insert_df_in_db(master_df_updated)
                            print("Codeblock für Eröffnung Long-Position durchgelaufen. Das Programm wird beendet")
                            sys.exit(0)
                            
                    else:
                        master_df_updated["in_position"].iloc[-1] = 0
                        insert_df_in_db(master_df_updated)
                        print("Limit-Buy-Order wurde nicht gefillt. Programm wird hier beendet.")
                        sys.exit(0)

            else:
                print(f"Platzierung der Order Fehlgeschlagen. Code: {buy_order['code']}")
                master_df_updated["in_position"].iloc[-1] = 0
                insert_df_in_db(master_df_updated)
                print("Order für Long-Position konnte nicht gesetzt werden. Das Programm wird beendet")
                sys.exit(0)

# Logik für eine Short-Position. Ähnlich wie bei Long, ein paar Kleinigkeiten werden angepasst (side bspw. und Positionsmarkierung).

    else:

        if __name__ == "__main__":                                  
            key = API_KEY
            secret = API_SECRET
            passphrase = API_PASSPHRASE
    
            session = requests.Session()
            signer = KcSigner(key, secret, passphrase)
    
            #mark_price = get_mark_price(signer, session, symbol="ICPUSDTM")
            sell_order = place_order(signer, session, client_oid="84734282939feeef", symbol="ICPUSDTM", 
                                     side="sell", typ="limit", 
                                     price=get_mark_price(signer, session, symbol="ICPUSDTM"), time_in_force="IOC", value = "100")

            time.sleep(10)
            
            if sell_order["code"] == "200000":
                print("Limit Sell-Order erfolgreich gesetzt")
                oid = sell_order["data"]["orderId"]

                if __name__ == "__main__":
                    key = API_KEY
                    secret = API_SECRET
                    passphrase = API_PASSPHRASE
                                                              
                    session = requests.Session()
                    signer = KcSigner(key, secret, passphrase)
                                                              
                    is_active, status, filled_quantity, avg_price = check_for_fill(signer, session, oid)
                    
                    time.sleep(2)

                    if status == "done" and filled_quantity != 0:
                        print("Limit Sell-Order erfolgreich gefillt.")

                        master_df_updated["in_position"].iloc[-1] = -1
                        master_df_updated["filled_price"].iloc[-1] = avg_price
                        master_df_updated["order_id"].iloc[-1] = oid
                                                              
                        if __name__ == "__main__":
                            key = API_KEY
                            secret = API_SECRET
                            passphrase = API_PASSPHRASE

                            session = requests.Session()
                            signer = KcSigner(key, secret, passphrase)

                            take_profit_order = place_take_profit(signer, session, 
                                                                  client_oid="84734282939selltp", 
                                                                  symbol="ICPUSDTM", side="buy", 
                                                                  typ="limit", 
                                                                  price=str(round(master_df_updated["sma_12"].iloc[-1], 3)), 
                                                                  quantity=(filled_quantity/100))

                            time.sleep(2)

                            master_df_updated["tp_order_id"].iloc[-1] = take_profit_order["data"]["orderId"] # Ggf. noch Check, ob Limit erfolgreich gesetzt.

                            print("TP-Order für neu eröffnete Position gesetzt" if take_profit_order["code"] == "200000" else "TP-Order für neu eröffnete Position konnte nicht gesetzt werden")

                            insert_df_in_db(master_df_updated)
                            print("Codeblock für Eröffnung Short-Position durchgelaufen. Das Programm wird beendet")
                            sys.exit()
                            
                    else:
                        master_df_updated["in_position"].iloc[-1] = 0
                        insert_df_in_db(master_df_updated)
                        print("Limit-Sell-Order wurde nicht gefillt. Programm wird hier beendet.")
                        sys.exit(0)

            else:
                print(f"Platzierung der Sell-Order Fehlgeschlagen. Code: {sell_order['code']}")	
                master_df_updated["in_position"].iloc[-1] = 0
                insert_df_in_db(master_df_updated)
                print("Order für Short-Position konnte nicht gesetzt werden. Das Programm wird beendet")
                sys.exit(0)

else:
    print("Kein neues Signal für eine Position.")
    # df["in_position"].iloc[-1] = 0 --> Wird hier wahrscheinlich nicht benötigt, weil das am Anfang definiert wurde
    insert_df_in_db(master_df_updated)
    print("Programm wird beendet. Kein neues Handelssignal. Letzter else-Block")
    sys.exit(0)


# DEN MASTER-DF ABSPEICHERN NICHT VERGESSEN. IMMER IN ABHÄNGIGKEIT VOM JEWEILIGEN CONTROL FLOW 
# "KILL-SWITCHES" EINBAUEN: WENN LIMIT NICHT GEFILLT, DF FILLEN UND SPEICHERN, DANN PROGRAMMABBRUCH

# Ende des Bots. Ausführung durch einen Cron-Job oder cloud-scheduler, der alle 30-Minuten startet. 


# In[285]:





# In[284]:




