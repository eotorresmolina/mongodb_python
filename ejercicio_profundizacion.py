'''
SQL Introducción [Python]
Ejercicio de profundización
---------------------------
Autor: Inove Coding School
Version: 1.1

Descripción:
Programa creado para poner a prueba los conocimientos
adquiridos durante la clase.
'''

__author__ = "Emmanuel O. Torres Molina"
__email__ = "emmaotm@gmail.com"
__version__ = "1.1"


import tinymongo as tm
import tinydb
import requests

import json
import os

from config import config


class TinyMongoClient(tm.TinyMongoClient):
    @property
    def _storage(self):
        return tinydb.storages.JSONStorage


script_path = os.path.dirname(os.path.realpath(__file__))

config_path_name = os.path.join(script_path, 'config.ini')

db = config('db', config_path_name)
url = config('requests', config_path_name)['url']


db_name = db['database']
collection_name = db['collection']


def clear( ):
    conn = TinyMongoClient()
    db = conn[db_name]
    db.collection_name.remove({})
    conn.close()


def fetch ( ):
    response_json = {}
    response = requests.get(url)
    if response.status_code == 200:
        response_json = response.json( )

    return response_json


def fill_chunk (chunksize=10):
    chunk = []

    conn = TinyMongoClient()
    db = conn[db_name]

    json_data = fetch()

    for row in json_data:
        chunk.append(row)
        if len(chunk) == chunksize:
            db.collection_name.insert_many(chunk)
            chunk.clear()

    if chunk:
        db.collection_name.insert_many(chunk)

    conn.close( )



def fill ( ):
    print('Completamos la Colección:')
    json_data = fetch()

    conn = TinyMongoClient()
    db = conn[db_name]
    db.collection_name.insert_many(json_data)
    conn.close( )


def show( ):
    print('\n\nMostramos el Contenido de la Colección:\n')
    conn = TinyMongoClient()
    db = conn[db_name]
    data = db.collection_name.find( )
    
    json_data = list(data)
    json_string = json.dumps(json_data, indent=4)

    conn.close()

    print('{}\n\n'.format(json_string))


def title_completed_count (userId):
    conn = TinyMongoClient()
    db = conn[db_name]
    count = db.collection_name.find({"userId": userId, "completed": True}).count()
    conn.close()

    return count
    

if __name__ == "__main__":
    
    # Borrar DB
    clear( )

    # Completar la DB con el JSON request
    #fill( )
    fill_chunk(chunksize=12)

    # Mostrar el Contenido de la DB
    show()

    # Títulos Completados.
    userId = 5
    cant_titles_completed = title_completed_count(userId)
    print("\n\nLa Cantidad de Títulos Completados por el UserId {} es: {}\n\n".format(userId, cant_titles_completed))
    