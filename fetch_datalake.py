import os
import pandas as pd
import logging


from azure.storage.blob import generate_blob_sas, BlobSasPermissions
from azure.storage.filedatalake import DataLakeServiceClient
from datetime import datetime, timedelta
import time
from dotenv import load_dotenv, dotenv_values 


# loading variables from .env file
load_dotenv() 





def _extract_account_details():
    """
    Extracts account details from environment variables
    """

    global account_name, account_key, container_data
    account_name = os.environ["DataLakeAccountName"]
    account_key = os.environ["DataLakeAccountKey"]
    container_data = os.environ["DataLakeDataContainer"]


def get_sas_url(container, blob_name):
    sas_i = generate_blob_sas(account_name=account_name,
                              container_name=container,
                              blob_name=blob_name,
                              account_key=account_key,
                              permission=BlobSasPermissions(read=True),
                              expiry=datetime.utcnow() + timedelta(hours=1))

    sas_url = 'https://' + account_name + '.blob.core.windows.net/' + container + '/' + blob_name + '?' + sas_i

    return sas_url


def get_data(ship_container, year_month):
    """
    Fetches data from Data Lake
    :param ship_container: Name of the ship which is also the name of folder which contains data
    :param year_month: List of dates in format 'YYYY/MM'. Helpful in extracting data from paths because of folder structure
    :return df: DF of signals data
    """
    _extract_account_details()
    connection_string = 'DefaultEndpointsProtocol=https;AccountName=' + account_name + ';AccountKey=' + account_key + ';EndpointSuffix=core.windows.net'
    datalake_service_client = DataLakeServiceClient.from_connection_string(connection_string)
    filesystem_client = datalake_service_client.get_file_system_client(container_data)

    df_instance = pd.DataFrame({})
    df = pd.DataFrame({})

    # Loop is used in case data has to be fetched from multiple months/years
    for i in range(0, len(year_month)):

        path_iterator = filesystem_client.get_paths(f'{ship_container}/{year_month[i]}')

        try:
            for path in path_iterator:
                path_name = path.name
                sas_url = get_sas_url(container_data, path_name)

                # Data can either be as a csv or a csv inside zip
                if '.zip' in sas_url:
                    df_instance = pd.read_csv(sas_url, compression='zip')
                elif '.csv' in sas_url:
                    df_instance = pd.read_csv(sas_url)

                df_instance = df_instance.dropna(how='all')

                if df.empty:
                    df = df_instance
                else:
                    df = pd.concat([df, df_instance], ignore_index=True)
            df = df.drop_duplicates(subset='ts', keep='first')
        except:
            continue

    # In some data, date format was different and needed to be set uniformly throughout DF.
    df.index = pd.to_datetime(df['ts'], format='ISO8601')

    return df


def fetch_data(ship_container: str, start_date: pd.Timestamp, end_date: pd.Timestamp) -> pd.DataFrame:
    year_month = pd.date_range(start_date, end_date, freq='D').strftime("%Y/%m").unique().tolist()
    df = get_data(ship_container, year_month)
    df['ts'] = pd.to_datetime(df['ts'])
    df = df.loc[(df['ts'] >= start_date.tz_localize('UTC')) & (df['ts'] < end_date.tz_localize('UTC'))]
    df.drop(df.columns[[0,1]], axis=1, inplace=True)

    return df


def main():
    timestr = time.strftime("%Y%m%d-%H%M%S")
    start_date = pd.to_datetime('2024-05-01')
    end_date = pd.to_datetime('2024-06-01')
    ship_container = 'your_folder name '
    year_month = pd.date_range(start_date, end_date, freq='D').strftime("%Y/%m").unique().tolist()
    df = get_data(ship_container, year_month)
    df['ts'] = pd.to_datetime(df['ts'])
    df = df.loc[(df['ts'] >= start_date.tz_localize('UTC')) & (df['ts'] < end_date.tz_localize('UTC'))]
    df.drop(df.columns[[0,1]], axis=1, inplace=True)
    df.reset_index('ts', inplace=True)
    df.to_csv("data/test_fetch"+ timestr +".csv")    
    


if __name__ == '__main__':
    main()
