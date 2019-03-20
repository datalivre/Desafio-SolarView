# _*_ coding:utf-8 _*_
# @author Robert Carlos                                 #
# email robert.carlos@linuxmail.org                     #
# 2019-Mar (CC BY 3.0 BR)                               #

import json
import os
import time

import numpy as np
import pandas as pd
import requests
from geopy.extra.rate_limiter import RateLimiter
from geopy.geocoders import Nominatim

import folium
import sqlalchemy

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


def clean_arrange(funcao_get_file, parameters='ALLSKY_SFC_SW_DWN',
                  identifier='Global', tempAverage='CLIMATOLOGY',
                  LATL=6, LATR=-34, LONU=-74, LOND=-34):
    """
    Remove columms that will not be used. Elimate missing data and 
    renames the remaining columms. Remove all the data group which 
    the entire dataset that is not between the defined latitude and longitude.
    arg
    ----
        * funcao_get_file: Receive the get_file function as a parameter.
        * LATL, LATR, LONU, LOND: bounding box pointed to Brazil as standard.
    """
    dataset = funcao_get_file(parameters, identifier, tempAverage)

    if 'PARAMETER' in dataset.columns:
        dataset.drop('PARAMETER', axis=1, inplace=True)

    dataset = pd.DataFrame(dataset[(dataset['LAT'] <= LATL) & (
        dataset['LAT'] >= LATR) & (dataset['LON'] >= LONU) & (dataset['LON'] <= LOND)])

    dataset.replace(-999, np.nan)
    dataset.dropna(inplace=True)
    dataset.rename(columns={
        'JAN': 'JANEIRO', 'FEB': 'FEVEREIRO', 'MAR': 'MARÇO', 'APR': 'ABRIL',
        'MAY': 'MAIO', 'JUN': 'JUNHO', 'JUL': 'JULHO', 'AUG': 'AGOSTO',
        'SEP': 'SETEMBRO', 'OCT': 'OUTUBRO', 'NOV': 'NOVEMBRO', 'DEC': 'DEZEMBRO'
    }, inplace=True)

    return dataset


dataset = clean_arrange(get_file)


def escreve_csv(lista):
    with open('fileforchoropleth.csv', 'a') as fobj:
        fobj.write(f'{lista}\n')


def get_address(dataset, alvo='ANN'):
    """
    Uses the coordinates of a dataset to return a .csv file 
    contained with columns as: country, state and town.
    arg
    ----
        * dataset: dataset received from API from NASA 
        contained with the climatologics values and coordinates.
        * alvo: dataset column that will be preserved and 
        recorded in a file that generate by this function.
    """
    geolocator = Nominatim(user_agent='NASA2000')
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1,
                          max_retries=300, error_wait_seconds=30)

    bbox = dataset[['LAT', 'LON', alvo]].astype(str)
    bbox['LATLON'] = bbox[['LAT', 'LON']].apply(lambda x: ','.join(x), axis=1)
    x = bbox['LATLON'].tolist()
    media = bbox['ANN'].tolist()

    lista_atrib = ['country', 'county',
                   'neighbourhood', 'state', 'suburb', 'town']
    contador = 0
    endereco = ''
    for coord in x:
        try:
            obj_geocoder = geolocator.reverse(coord, timeout=300)
            if obj_geocoder is not None:
                tmp_atrib = obj_geocoder.raw
                endereco += coord + ';' + media[contador] + ';'
                if 'address' in tmp_atrib:
                    for item in lista_atrib:
                        endereco += tmp_atrib['address'][item] + \
                            ';' if item in tmp_atrib['address'] else ';'
                else:
                    endereco += ';;;;;'
            else:
                endereco += ';;;;;;;'
        except Exception as e:
            time.sleep(10)
            print(f'{e} ' + str(contador))
        print(endereco)
        escreve_csv(endereco)
        endereco = ''
        contador += 1


get_address(dataset)


query = """
    create table if not exists global(
        id int not null auto_increment, latlon decimal(9,6) not null,
        media decimal(6,3) not null,
        country text, county text, neighbourhood text, state text,
        suburb text, town text, primary key (id));
"""


def insert_data(user, password, query='', db='solar_nasa', table='global'):
    """
    Insert the data in a MySQL database.
    arg
    ----
        * user: username in the database.
        * password: password to the database.
        * query: string to create a spread sheet in the database.
        * db: the name in the database.
        * table: the name in the spread sheet.
    """

    dataset = pd.read_csv('fileforchoropleth.csv', sep=';', header=0)
    dataset.dropna(how='all', axis=1, inplace=True)
    dataset.columns = ['latlon', 'media', 'country', 'county', 'neighbourhood',
                       'state', 'suburb', 'town']

    try:
        engine = sqlalchemy.create_engine(
            f'mysql+pymysql://{user}:{password}@localhost:3306/{db}')
        with engine.connect() as con:
            con.execute(query)
            dataset.to_sql(
                name=table,
                con=con,
                index=False,
                if_exists='replace')
    except Exception as e:
        print('Houve um erro ao conectar ao banco de dados: {e}')
    finally:
        engine.dispose()


insert_data(user='root', password=1984, query=query)


def select_data(fields, tables, user, password, where=None,
                db='solar_nasa', table='global', query=None):
    """
    Capture data from MySQL database through a search.
    Return a dataframe.
    arg
    ----
        * fields, tables, where: fields for creating a search.
    """

    if fields and tables is not None:
        query = f'select {fields} from {tables}'
        if where:
            query += f' where {where}'
        query += ';'
    print(query)
    try:
        engine = sqlalchemy.create_engine(
            f'mysql+pymysql://{user}:{password}@localhost:3306/{db}')
        if query:
            return pd.read_sql_query(query, engine)
        else:
            return pd.read_sql(table, engine)
    except Exception as e:
        print('Houve um erro ao conectar ao banco de dados: {e}')
    finally:
        engine.dispose()


state_data = select_data(fields='state, media', tables='global',
                         where='country="Brasil"', user='root', password=1984)


state_geo = os.path.join('br-states.json')

m = folium.Map(
    width=600, height=600,
    location=[-16.1303, -56.6098],
    zoom_start=4)

folium.Choropleth(
    geo_data=state_geo,
    name='Insolation incident',
    data=state_data,
    columns=['state', 'media'],
    key_on='feature.id',
    fill_color='YlOrBr',
    fill_opacity=0.7,
    line_opacity=0.1,
    highlight=True,
    legend_name='Insolation Incident on a Horizontal Surface'
).add_to(m)

folium.LayerControl().add_to(m)

m.save('index1.html')
