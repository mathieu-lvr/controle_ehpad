# -*- coding: utf-8 -*-
"""
Created on Tue Jan 17 10:30:45 2023

@author: mathieu.olivier
"""


# Modules à installer
import os
from os import listdir
import pandas as pd 

#Regle à respecter fichier excel seulement
#La feuille d'interet doit etre placée en premier
#Les noms de colonne doivent être ne première ligne et aucune colonne ne doit être vide sur la gauche


def checkIfPathExists(file):
    if os.path.exists(file):
        os.remove(file)
        print('Ancien fichier écrasé')
        

def _convertXlsxToCsv(inputExcelFilePath):
    try:
    # Reading an excel file
    #   sheetname = getSheetName()
        excelFile = pd.read_excel(inputExcelFilePath, header=0)
    # Create the csv path
        outputCsvFilePath = inputExcelFilePath[:-5]+".csv"
        checkIfPathExists(outputCsvFilePath)
    # Converting excel file into CSV file
        excelFile.to_csv(outputCsvFilePath, index = None, header=True, sep=';', encoding='UTF-8')
        return outputCsvFilePath
    except ValueError as err:
        print(err)
        return str(err) 

#_convertXlsxToCsv("C:/Users/mathieu.olivier/Documents/Helios/Script_V2/input/Calcul du nombre de signalements.xlsx")

def _csvReader(csvFilePath):
    df = pd.read_csv(csvFilePath, sep= ';', encoding='UTF-8',low_memory=False)
    return df
#Checker utf8 
#Pousser le csv sans mettre en dataframe



### Partie nettoyage des données

import re


def _cleanSrcData(df):
# Enlever caractères spéciaux, accents, espace ( _ ) ,
    return df


csvFilePath = 'C:/Users/mathieu.olivier/Documents/Helios/Script_V2/input/export-tdbesms-2020-region_agrege.csv'

df = pd.read_csv(csvFilePath, sep=';', encoding='latin-1')




### Partie Création de la DB et ajout des tables

import sqlite3

dbname = 'controle_ehpad'

def checkIfDBExists(dbname):
    if os.path.exists(dbname + '.sqlite'):
        os.remove(dbname + '.sqlite')
        print('Ancienne base de donnée écrasée')

def _initDb(dbname):
    #Supprime l'ancienne base de donnée
    checkIfDBExists(dbname)
    #Crée la nouvelle base de donnée
    conn = sqlite3.connect(dbname + '.sqlite')
    conn
    print('Création de la base de donnée {}.sqlite '.format(dbname))
    return conn

conn = _initDb(dbname)

def _importSrcData(df, table_name):
    df.to_sql(name=table_name, con=conn)
    print('La table {} a été ajouté à la base de donnée {}'.format(table_name,dbname))
    return 

def _executeTransform():
    #Appeler les requetes sql
    return 




allSources = listdir('input/sources')
allExcelSources = [path for path in allSources if path.split('.')[-1].lower()=='xlsx']

for inputExcelFilePath in allExcelSources:
    _importSrcData(
        _cleanSrcData(
            _csvReader(
                _convertXlsxToCsv('input/sources/'+inputExcelFilePath)
            )
        ),
        inputExcelFilePath.split('/')[-1].split('.')[0]
        )
    
conn.close()

#%% Update section
#Pour Update une table à partir d'un fichier

def _updateTable(dbname, table_name, excelFilePath):
    try:
        sqliteConnection = sqlite3.connect(dbname+'.sqlite')
        cursor = sqliteConnection.cursor()
        print("Connected to {}".format(dbname))

        #sql_drop_table = "DROP TABLE {}".format(table_name)
        cursor.execute("DROP TABLE "+table_name)
        sqliteConnection.commit()
        print("Table {} dropped".format(table_name))
        _importSrcData(
            _cleanSrcData(
                _csvReader(
                    _convertXlsxToCsv(excelFilePath)
                )
            ),
            table_name
            )
        print("Table {} Updated".format(table_name))
        cursor.close()

    except sqlite3.Error as error:
        print("Failed to update sqlite table", error)
    finally:
        if sqliteConnection:
            sqliteConnection.close()
            print("The SQLite connection is closed")
table_name = '20220624_Diamant_MS_Demande_BD'
_updateTable(dbname, table_name, "input/sources/20220624_Diamant_MS_Demande_BD.xlsx" )

'''
Sources: 
    - input_xslx
    - input_csx
    - ref
'''

#%% Unicode test

from unidecode import unidecode

print(unidecode('élé'))

print(unidecode('kožušček'))
