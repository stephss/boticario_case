import datetime
import pandas as pd
from google.cloud import storage
from airflow import models
from airflow.operators import python_operator

default_dag_args = {
    'start_date': datetime.datetime(2022, 11, 25),
}

with models.DAG(
        'ingestao_tb_venda_marca_linha',
        schedule_interval=datetime.timedelta(days=1),
        default_args=default_dag_args) as dag:
    def runner():
        table_id = 'trusted.tb_venda_marca_linha'

        bucket_name = "datalake_boticario"

        files = ['landzone/Base 2017.xlsx', 'landzone/Base_2018.xlsx', 'landzone/Base_2019.xlsx']

        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(files[0])
        data_bytes = blob.download_as_bytes()
        excel_data = pd.read_excel(data_bytes)
        for i in range(1, len(files)):
            blob = bucket.blob(files[i])
            data_bytes = blob.download_as_bytes()
            excel_data = pd.concat([excel_data, pd.read_excel(data_bytes)], axis=0)

        excel_data['ANO'] = pd.to_datetime(excel_data['DATA_VENDA'], format='%Y%m%d').dt.year
        excel_data['MES'] = pd.to_datetime(excel_data['DATA_VENDA'], format='%Y%m%d').dt.month

        raw_df = pd.DataFrame(excel_data,
                              columns=['ID_MARCA', 'MARCA', 'ID_LINHA', 'LINHA', 'DATA_VENDA', 'QTD_VENDA', 'ANO',
                                       'MES'])
        df = raw_df.drop_duplicates(keep='last', subset=['ID_MARCA', 'ID_LINHA', 'DATA_VENDA'])
        groupedData = df.groupby(
            [
                df['MARCA'],
                df['LINHA']
            ],
            as_index=False
        ).agg({'QTD_VENDA': 'sum', 'MARCA': 'first', 'LINHA': 'first'})
        groupedData.columns = ['total_venda', 'marca', 'linha']
        groupedData.to_gbq(table_id, if_exists='replace')


    populate_table = python_operator.PythonOperator(
        task_id='ingestao_tb_venda_marca_linha',
        python_callable=runner)
