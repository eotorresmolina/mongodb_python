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


class TinyMongoClient(tm.TinyMongoClient):
    @property
    def _storage(self):
        return tinydb.storages.JSONStorage

db_name = "course"


def clear( ):
    conn = TinyMongoClient()
    db = conn[db_name]
    db.titles.remove({})
    conn.close()


def fetch ( ):
    url = 'https://jsonplaceholder.typicode.com/todos'
    response = requests.get(url)
    return response.json()


def fill_chunk (chunksize=10):
    chunk = []

    conn = TinyMongoClient()
    db = conn[db_name]

    json_data = fetch()

    for row in json_data:
        chunk.append(row)
        if len(chunk) == chunksize:
            db.titles.insert_many(chunk)
            chunk.clear()

    if chunk:
        db.titles.insert_many(chunk)

    conn.close( )



def fill ( ):
    print('Completamos la Colección:')
    json_data = fetch()

    conn = TinyMongoClient()
    db = conn[db_name]
    db.titles.insert_many(json_data)
    conn.close( )


def show( ):
    print('\n\nMostramos el Contenido de la Colección:\n')
    conn = TinyMongoClient()
    db = conn[db_name]
    data = db.titles.find( )
    
    json_data = list(data)
    json_string = json.dumps(json_data, indent=4)

    conn.close()

    print('{}\n\n'.format(json_string))


def title_completed_count (userId):
    conn = TinyMongoClient()
    db = conn[db_name]
    count = db.titles.find({"userId": userId, "completed": True}).count()
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
    print("La Cantidad de Títulos Completados por el UserId {} es: {}\n\n".format(userId, cant_titles_completed))
    