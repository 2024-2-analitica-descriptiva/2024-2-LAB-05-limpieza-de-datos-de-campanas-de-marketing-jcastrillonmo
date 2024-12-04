"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel

""" En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months """

import os
import pandas as pd
from zipfile import ZipFile

def clean_campaign_data():
    # Directorios
    input_dir = "files/input/"
    output_dir = "files/output/"

    # Asegurarse de que el directorio de salida exista
    os.makedirs(output_dir, exist_ok=True)

    # Crear listas para almacenar los datos procesados
    client_data = []
    campaign_data = []
    economics_data = []

    # Procesar los archivos comprimidos en la carpeta de entrada
    for file_name in os.listdir(input_dir):
        if file_name.endswith(".zip"):
            zip_path = os.path.join(input_dir, file_name)
            with ZipFile(zip_path, 'r') as zip_file:
                for csv_name in zip_file.namelist():
                    with zip_file.open(csv_name) as csv_file:
                        df = pd.read_csv(csv_file)

                        # Procesar columnas para cada archivo
                        client_data.append(df[['client_id', 'age', 'job', 'marital', 'education', 'credit_default', 'mortgage']].copy())
                        campaign_data.append(df[['client_id', 'number_contacts', 'contact_duration', 
                                                 'previous_campaign_contacts', 'previous_outcome', 
                                                 'campaign_outcome', 'day', 'month']].copy())
                        economics_data.append(df[['client_id', 'cons_price_idx', 'euribor_three_months']].copy())

    # Unir los datos y procesarlos
    client_df = pd.concat(client_data, ignore_index=True)
    campaign_df = pd.concat(campaign_data, ignore_index=True)
    economics_df = pd.concat(economics_data, ignore_index=True)

    # Procesar client.csv
    client_df['job'] = client_df['job'].str.replace('.', '', regex=False).str.replace('-', '_', regex=False)
    client_df['education'] = client_df['education'].str.replace('.', '_', regex=False).replace("unknown", pd.NA)
    client_df['credit_default'] = client_df['credit_default'].apply(lambda x: 1 if x == "yes" else 0)
    client_df['mortgage'] = client_df['mortgage'].apply(lambda x: 1 if x == "yes" else 0)

    # Procesar campaign.csv
    campaign_df['previous_outcome'] = campaign_df['previous_outcome'].apply(lambda x: 1 if x == "success" else 0)
    campaign_df['campaign_outcome'] = campaign_df['campaign_outcome'].apply(lambda x: 1 if x == "yes" else 0)
    campaign_df['last_contact_date'] = pd.to_datetime(
        campaign_df['day'].astype(str) + '-' + campaign_df['month'] + '-2022',
        format='%d-%b-%Y'
    ).dt.strftime('%Y-%m-%d')
    campaign_df = campaign_df.drop(columns=['day', 'month'])

    # Guardar archivos
    client_df.to_csv(os.path.join(output_dir, "client.csv"), index=False)
    campaign_df.to_csv(os.path.join(output_dir, "campaign.csv"), index=False)
    economics_df.to_csv(os.path.join(output_dir, "economics.csv"), index=False)

    return


if __name__ == "__main__":
    clean_campaign_data()