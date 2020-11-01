#!/usr/bin/env python
'''
MongoDB [Python]
Ejemplos de clase
---------------------------
Autor: Inove Coding School
Version: 1.2

Descripcion:
Programa creado para mostrar ejemplos prácticos de los visto durante la clase
'''

__author__ = "Emmanuel O. Torres Molina"
__email__ = "emmaotm@gmail.com"
__version__ = "1.2"

import json

import tinymongo as tm
import tinydb

# Bug: https://github.com/schapman1974/tinymongo/issues/58
class TinyMongoClient(tm.TinyMongoClient):
    @property
    def _storage(self):
        return tinydb.storages.JSONStorage

db_name = 'personas'


def clear():
    conn = TinyMongoClient()
    db = conn[db_name]

    # Eliminar todos los documentos que existan en la coleccion persons
    db.persons.remove({})

    # Cerrar la conexión con la base de datos
    conn.close()


def insert_persona(name, age, nationality=""):
    conn = TinyMongoClient()
    db = conn[db_name]

    # Insertar un documento
    persona_json = {"name": name, "age": age, "nationality": nationality}
    db.persons.insert_one(persona_json)


    # Cerrar la conexión con la base de datos
    conn.close()


def insert_grupo(group):
    conn = TinyMongoClient()
    db = conn[db_name]

    # Insertar varios documentos, una lista de JSON
    db.persons.insert_many(group)

    # Cerrar la conexión con la base de datos
    conn.close()


def show(fetch_all=True):
    # Conectarse a la base de datos
    conn = TinyMongoClient()
    db = conn[db_name]

    # Leer todos los documentos y obtener todos los datos juntos
    if fetch_all is True:
        cursor = db.persons.find()
        data = list(cursor)
        json_string = json.dumps(data, indent=4)
        print(json_string)
    else:
        # Leer todos los documentos y obtener los datos de a uno
        cursor = db.persons.find()
        for doc in cursor:
            print(doc)

    # Cerrar la conexión con la base de datos
    conn.close()


def find_persona(name):
    # Conectarse a la base de datos
    conn = TinyMongoClient()
    db = conn[db_name]

    # Encontrar un documento por el campo "name"
    person_data = db.persons.find_one({"name": name})

    # Cerrar la conexión con la base de datos
    conn.close()
    return person_data


def count_by_country(country):
    # Conectarse a la base de datos
    conn = TinyMongoClient()
    db = conn[db_name]

    # Contar cuantos documentos poseen el campo de nacionalidad indicado
    count = db.persons.find({"nationality": country}).count()

    # Cerrar la conexión con la base de datos
    conn.close()
    return count


def lookfor_older_than(age):
    conn = TinyMongoClient()
    db = conn[db_name]

    # Leer todos los documentos y obtener los datos de a uno
    cursor = db.persons.find({"age": {"$gt": age}})
    for doc in cursor:
        print(doc)


def update_persona_address(name, address):
    # Conectarse a la base de datos
    conn = TinyMongoClient()
    db = conn[db_name]

    # Actualizar un documento que coincida con el campo name
    db.persons.update_one({"name": name}, {"$set": address})

    # Cerrar la conexión con la base de datos
    conn.close()


def remove_persona(name):
    # Conectarse a la base de datos
    conn = TinyMongoClient()
    db = conn[db_name]

    # Remover todos los documentos que poseen el campo name deseado
    db.persons.remove({"name": name})
    # Cerrar la conexión con la base de datos
    conn.close()


if __name__ == '__main__':
    print("Bienvenidos a otra clase de Inove con Python")
    # Borrar la DB
    clear()

    # Fill database
    insert_persona('Inove', 12, 'Argentina')
    insert_persona('Python', 29, 'Holanda')
    insert_persona('Max', 35, 'Estados Unidos')
    insert_persona('Mirta', 93, 'Argentina')

    # Mostrar contenido
    show()

    # Modificar contenido de "Inove", agregar dirección
    # ------------------------------------------------
    inove_data = find_persona('Inove')

    address = {"address": {"street": "Monroe", "number": 500}}
    update_persona_address('Inove', address)

    inove_data = find_persona('Inove')
    # ------------------------------------------------

    # Contar cuantos argentinos en la db
    print('Cantidad de argentinos:', count_by_country("Argentina"))

    # Contar cuantas personas son mayores de 25
    lookfor_older_than(25)

    # Insertar un grupo de datos
    # ------------------------------------------------
    group = [{"age": 40, "nationality:": "Estados Unidos"},
             {"name": "SQL", "age": 13, "nationality:": "Inglaterra"},
             {"name": "SQLite", "nationality:": "Estados Unidos"}
             ]

    insert_grupo(group)
    print('\n\nMostrar nuevos datos insertados por grupo')
    show(False)
    # ------------------------------------------------
