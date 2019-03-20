# _*_ coding:utf-8 _*_
# @author Robert Carlos                                 #
# email robert.carlos@linuxmail.org                     #
# 2019-Mar (CC BY 3.0 BR)                               #

import json

import pandas as pd
import requests

URL = 'https://power.larc.nasa.gov/cgi-bin/v1/DataAccess.py?request=execute&userCommunity=SSE&outputList=CSV'


def get_file(parameters, identifier, tempAverage):
    """
    From the custom NASA API link that leads to a json file, 
    this function find the file, convert and return the data to a dataframe.
    args
    ----
        * parameters: Parameters based on solar radiation adapted to renewable
        solar energy communities, efficient energy, sustainable construction and agriculture.
        * identifier:
            * SinglePoint: returns to a time series based on a single coordinate in the time interval provided.
            * Regional: returns to a time series based on a bounding box over the time interval provided. 
            * Global: returns long-term climate measures across the globe.       
        * tempAverage:
            * DAILY: annual daily average.
            * INTERANNUAL: average monthly and annual per year.
            * CLIMATOLOGY: monthly long-term averages.
    """
    payload = {'parameters': parameters,
               'identifier': identifier, 'tempAverage': tempAverage}
    r = requests.get(URL, params=payload)
    print(r.url)
    try:
        if r.status_code == 200:
            base = json.loads(r.text)
            url = base['outputs']
            return pd.read_csv(url['csv'], sep=',', skiprows=8)
    except Exception as e:
        print(f'Ocorreu um erro ao tentar abrir a url: {e}')
    finally:
        print(f'Status code: {r.status_code}')

