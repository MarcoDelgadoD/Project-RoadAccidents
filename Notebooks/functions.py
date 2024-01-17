# Este script contiene funciones útiles para facilitar el proceso de
# Extracción, Transformación y Carga (ETL).


# Importación de bibliotecas


import pandas as pd
import numpy as np
import mysql.connector
from sqlalchemy import create_engine


# Funciones


"""
Definimos una función para cargar datos desde un archivo Excel
"""
def cargar_archivos_excel(archivo, hojas):

    # Utiliza pd.ExcelFile para cargar el archivo Excel
    xls_file = pd.ExcelFile(archivo)

    # Inicializamos un diccionario para almacenar DataFrames de cada hoja
    dfs = {}

    # Iteramos sobre las hojas especificadas
    for hoja in hojas:

        # Leemos los datos de la hoja actual y los almacenamos en un DataFrame
        df = pd.read_excel(xls_file, hoja)

        # Agregamos el DataFrame al diccionario usando el nombre de la hoja como clave
        dfs[hoja] = df

    # Devolvemos el diccionario que contiene DataFrames de cada hoja
    return dfs


"""
Definimos una función para analizar la presencia de valores 'SD' en cada columna de los DataFrames
"""
def analizar_valores_sd(dataframe):

    # Obtenemos las columnas del DataFrame
    columnas_con_sd = dataframe.columns

    # Inicializamos una lista para almacenar los resultados
    resultados = []

    # Iteramos sobre las columnas que contienen "SD"
    for columna in columnas_con_sd:

        # Calculamos la cantidad de 'SD' en la columna actual
        cantidad_sd = dataframe[columna].eq("SD").sum()

        # Calculamos el porcentaje de "SD" en la columna actual
        porcentaje_sd = (cantidad_sd / len(dataframe)) * 100

        # Agregamos los resultados a la lista
        resultados.append({"Columna": columna, "Cantidad de SD": cantidad_sd, "Porcentaje de SD": porcentaje_sd})

    # Creamos un nuevo DataFrame a partir de la lista de resultados
    resultados_df = pd.DataFrame(resultados)

    # Filtramos las filas que tienen al menos un valor "SD"
    resultados_con_sd = resultados_df[resultados_df["Cantidad de SD"] > 0]

    # Obtenemos el DataFrame con los resultados
    return resultados_con_sd


"""
Definimos una función para realizar el proceso de limpieza de los datos de un DataFrame
"""
def data_cleaning(df,
                  drop_duplicates=False,
                  drop_na=False,
                  fill_na=None,
                  convert_to_datetime=None,
                  uppercase_columns=None,
                  lowercase_columns=None,
                  titlecase_columns=None,
                  strip_spaces=True,
                  rename_columns=None,
                  drop_columns=None,
                  categorize_columns=None,
                  replace_values=None,
                  new_columns=None,
                  new_columns2=None,
                  convert_date_columns=None, 
                  convert_to_int_columns=None,
                  convert_to_float=None,
                  ):
    
    cleaned_df = df.copy()

    # Elimina duplicados
    if drop_duplicates:
        cleaned_df.drop_duplicates(inplace=True)
        
    # Elimina filas con valores nulos
    if drop_na:
        cleaned_df.dropna(inplace=True)
        
    # Rellena valores nulos
    if fill_na:
        cleaned_df.fillna(fill_na, inplace=True)

    # Convierte columnas a tipo datetime
    if convert_to_datetime:
        for column in convert_to_datetime:
            cleaned_df[column] = pd.to_datetime(cleaned_df[column], errors='coerce')

    # Convierte columnas a mayúsculas
    if uppercase_columns:
        for column in uppercase_columns:
            cleaned_df[column] = cleaned_df[column].str.upper()
        
    # Convierte columnas a minúsculas
    if lowercase_columns:
        for column in lowercase_columns:
            cleaned_df[column] = cleaned_df[column].str.lower()
        
    # Convierte columnas a formato de título
    if titlecase_columns:
        for column in titlecase_columns:
            cleaned_df[column] = cleaned_df[column].str.title()
            
    # Trata columnas con espacios
    if strip_spaces:
        cleaned_df = cleaned_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        
    # Renombra columnas
    if rename_columns:
        cleaned_df.rename(columns=rename_columns, inplace=True)
        
    # Elimina columnas
    if drop_columns:
        cleaned_df.drop(columns=drop_columns, inplace=True)
        
    # Categoriza columnas
    if categorize_columns:
        for column in categorize_columns:
            if column in cleaned_df.columns:
                cleaned_df[column] = cleaned_df[column].astype('category')
            else:
                print(f"La columna '{column}' no existe en el DataFrame.")
                
    # Reemplaza valores en columnas
    if replace_values:
        for column, replacements in replace_values.items():
            cleaned_df[column].replace(replacements, inplace=True)
            
    # Agrega nuevas columnas
    if new_columns:
        for column, value in new_columns.items():
            cleaned_df[column] = value
            
    # Agregar nuevas columnas basadas en otras columnas
    if new_columns2:
        for new_column, column_expr in new_columns2.items():
            # Verificar si la expresión es proporcionada
            if column_expr:
                cleaned_df[new_column] = cleaned_df.eval(column_expr)
            else:
                cleaned_df[new_column] = None  # O cualquier valor predeterminado que prefieras
                  
    # Convierte columnas de fecha con formato específico
    if convert_date_columns:
        for column, date_format in convert_date_columns.items():
            cleaned_df[column] = pd.to_datetime(cleaned_df[column], format=date_format, errors='coerce')

    # Convierte columnas a tipo de dato entero
    if convert_to_int_columns:
        for column in convert_to_int_columns:
            cleaned_df[column] = pd.to_numeric(cleaned_df[column], errors='coerce').astype('Int64')
    
    # Convierte columnas a tipo float
    if convert_to_float:
        for column in convert_to_float:
            cleaned_df[column] = cleaned_df[column].astype(float)
        
    # Obtenemos el DataFrame con los resultados
    return cleaned_df


"""
Definimos una función para convertir una cadena de fecha ("fecha_str") a un objeto de tipo datatime de pandas
"""
def convertir_a_datetime(fecha_str):
    # Intentamos convertir al formato "YYYY-MM-DD"
    try:
        return pd.to_datetime(fecha_str, format="%Y-%m-%d")
    except ValueError:
        pass

    # Intentamos convertir al formato "MM/DD/YYYY"
    try:
        return pd.to_datetime(fecha_str, format="%m/%d/%Y")
    except ValueError:
        pass

    # Intentamos convertir al formato "YYYY-MM-DD 00:00:00"
    try:
        return pd.to_datetime(fecha_str, format="%Y-%m-%d %H:%M:%S")
    except ValueError:
        pass

    # Si no coincide con ningún formato conocido, retornar NaT
    return pd.NaT