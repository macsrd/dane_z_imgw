import requests
import xml.etree.ElementTree as ET
import pandas as pd
import pyodbc

#Funkcja, która ma za zadanie połączyć nas z bazą danych (zmienne server i database są zadeklarowane później).
def connect_to_database(server, database):
    conn = pyodbc.connect( 
    r'driver={ODBC Driver 17 for SQL Server};'
    r'server=' + server + 
    r';database=' + database +
    r';trusted_connection=yes;'
    )
    
    return conn

#Funkcja, która ma za zadanie pobrać dane z adresu URL, jeśli odpowiedź z serwera będzie "OK".
def download_data(url):
    response = requests.get(url)
    if response.status_code == 200:
        return response.content
    else:
        return None


#Funkcja konwertująca XML do formatu DataFrame.
def xml_to_df(xml_data):
    root = ET.fromstring(xml_data)
    data = []
    for item in root.findall('item'):
        row = {}
        for child in item:
            row[child.tag] = child.text
        data.append(row)
    df = pd.DataFrame(data)
    return df

#Funkcja tworząca tabelę i widok w bazie danych, a następnie wstawiająca dane do tabeli.
def insert_into_db(cnxn, df, table_name):
    cursor = cnxn.cursor()
    create_table_query = '''
    IF OBJECT_ID(N'dbo.IMGW', N'U') IS NULL
    BEGIN
        CREATE TABLE dbo.IMGW (
            id_stacji INT PRIMARY KEY,
            data_pomiaru NVARCHAR(50),
            godzina_pomiaru INT,
            temperatura DECIMAL(5,2),
            predkosc_wiatru INT,
            kierunek_wiatru INT,
            wilgotnosc_wzgledna DECIMAL(5,2),
            suma_opadu DECIMAL(5,2),
            cisnienie DECIMAL(5,1)
        )
    END
    '''
    create_view_query = '''

        CREATE VIEW vw_imgw AS 
            SELECT
            [id_stacji],
            [data_pomiaru],
            [godzina_pomiaru],
            [temperatura],
            [predkosc_wiatru],
            [kierunek_wiatru],
            [wilgotnosc_wzgledna],
            [suma_opadu],
            [cisnienie],
            ABS(cisnienie - 1013.25) as roznica_do_wzorcowego
        FROM dbo.imgw;
    '''
    cursor.execute(create_table_query)
    cursor.execute(create_view_query)
    for index, row in df.iterrows():
        cursor.execute(f"INSERT INTO {table_name} (id_stacji, data_pomiaru, godzina_pomiaru, temperatura, predkosc_wiatru, kierunek_wiatru, wilgotnosc_wzgledna, suma_opadu, cisnienie) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                       row['id_stacji'], row['data_pomiaru'], row['godzina_pomiaru'], row['temperatura'], row['predkosc_wiatru'], row['kierunek_wiatru'], row['wilgotnosc_wzgledna'], row['suma_opadu'], row['cisnienie'])
    cursor.commit()

#Skrypt definiujący zmienne i uruchamiający zadeklarowane wcześniej funkcje w celu pobrania danych z adresu URL, przekonwertowania XML na DataFrame oraz załadowania do bazy danych.
if __name__ == "__main__":
    
    url = 'https://danepubliczne.imgw.pl/api/data/synop/format/xml'
    server = 'localhost'
    database = 'Northwind'
    table_name = 'dbo.IMGW'

    xml_data = download_data(url)
    if xml_data:
        df = xml_to_df(xml_data)

        conn = connect_to_database(server, database)

        insert_into_db(conn, df, table_name)
        print("Dane zostaly zaladowane poprawnie")
    else:
        print("Nie udalo sie pobrac danych XML.")
