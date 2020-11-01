#!/usr/bin/env python
'''
SQL Introducción [Python]
Ejercicios de práctica
---------------------------
Autor: Inove Coding School
Version: 1.1

Descripcion:
Programa creado para poner a prueba los conocimientos
adquiridos durante la clase
'''

__author__ = "Emmanuel O. Torres Molina"
__email__ = "emmaotm@gmail.com"
__version__ = "1.1"


import json
import tinymongo as tm
import tinydb

# Bug: https://github.com/schapman1974/tinymongo/issues/58
class TinyMongoClient(tm.TinyMongoClient):
    @property
    def _storage(self):
        return tinydb.storages.JSONStorage

db_name = 'secundaria'


def clear():
    conn = TinyMongoClient()
    db = conn[db_name]

    # Eliminar todos los documentos que existan en la coleccion estudiante
    db.estudiante.remove({})

    # Cerrar la conexión con la base de datos
    conn.close()


def fill():
    print('Completemos esta tablita!\n\n')
    # Llenar la coleccion "estudiante" con al menos 5 estudiantes
    # Cada estudiante tiene los posibles campos:
    # id --> este campo es auto completado por mongo
    # name --> El nombre del estudiante (puede ser solo nombre sin apellido)
    # age --> cuantos años tiene el estudiante
    # grade --> en que año de la secundaria se encuentra (1-6)
    # tutor --> nombre de su tutor

    # Se debe utilizar la sentencia insert_one o insert_many.

    conn = TinyMongoClient()
    db = conn[db_name]

    group = [
                {"name": "Martin Miguel", "age": 28, "grade": 2, "tutor": "Ted Mosby"}, 
                {"name": "Carlos Catan", "age": 16, "grade": 1, "tutor": "Franco Pessana"}, 
                {"name": "Barney Stinson", "age": 18, "grade": 3, "tutor": "Jirafales"}, 
                {"name": "Oscar Torres", "grade": 2, "tutor": "Horacio Craiem"}, 
                {"name": "Mercedes Maldonado", "age": 27, "grade": 6, "tutor": "Mariano Llamedo Soria"}, 
                {"name": "Victoria Rodriguez", "age": 21, "grade": 5, "tutor": "Horacio Craiem"},
                {"name": "Michael Corleone", "age": 20, "grade": 5, "tutor": "Franco Pessana"}, 
                {"name": "Andrea Tattaglia", "grade": 4, "tutor": "Jirafales"}
            ]

    db.estudiante.insert_many(group)
    conn.close()


def show():
    print('Comprobemos su contenido, ¿qué hay en la tabla?\n')
    # Utilizar la sentencia find para imprimir en pantalla
    # todos los documentos de la DB
    # Queda a su criterio serializar o no el JSON "dumps"
    # para imprimirlo en un formato más "agradable"

    # Me conecto a la DB.
    conn = TinyMongoClient()
    db = conn[db_name]

    # Obtengo el cursor a la colección estudiante.
    cursor = db.estudiante.find()
    json_data = list(cursor)

    # Obtengo el json string y lo muestro en la terminal.
    json_string = json.dumps(json_data, indent=4)
    print('{}\n\n'.format(json_string))

    # Cierro la Conexión con la DB
    conn.close()


def find_by_grade(grade):
    print('Operación búsqueda!\n\n')
    # Utilizar la sentencia find para imprimir en pantalla
    # aquellos estudiantes que se encuentra en el año "grade"

    # De la lista de esos estudiantes debe imprimir
    # en pantalla unicamente los siguiente campos por cada uno:
    # id / name / age

    # Me conecto a la Base de Datos (DB):
    conn = TinyMongoClient()
    db = conn[db_name]

    cursor = db.estudiante.find({"grade": grade})
    data = [{"_id": row.get("_id"), "name": row.get("name"), "age": row.get("age")}
            for row in cursor]

    json_string = json.dumps(data, indent=4) 
    print("{}\n\n".format(json_string))

    # Cierro la Conexión con la Base de Datos(DB).
    conn.close()


def insert(student):
    print('Nuevos ingresos!\n\n')
    # Utilizar la sentencia insert_one para ingresar nuevos estudiantes
    # a la secundaria

    # El parámetro student deberá ser un JSON el cual se inserta en la db

    conn = TinyMongoClient()
    db = conn[db_name]
    db.estudiante.insert_one(student)
    conn.close()


def count(grade):
    print('Contar estudiantes: ', end='')
    # Utilizar la sentencia find + count para contar
    # cuantos estudiantes pertenecen el grado "grade"
    conn = TinyMongoClient()
    db = conn[db_name]
    count = db.estudiante.find({"grade": grade}).count()
    conn.close( )
    return count


if __name__ == '__main__':
    print("\n\nBienvenidos a otra clase de Inove con Python\n\n")
    # Borrar la db
    clear()

    fill()
    show()

    grade = 2
    find_by_grade(grade)

    student = {"name": "Lucas", "age": 22, "grade": 5, "tutor": "Florencia"}
    insert(student)
    show()

    cant = count(grade)
    print('{}\n\n'.format(cant))
