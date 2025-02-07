from airflow.models import DAG
import pendulum
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.macros import ds_add
from datetime import datetime,timedelta
from os.path import join
import pandas as pd
import os # aquivos e caminhos

def extrai_dados(data_interval_end):
    city = 'Itapevi,SP,BR'
    key = '8G8BQLV8BRKHYJN4TCVYJXXB6'

    URL = join('https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/',
                f'{city}/{data_interval_end}/{ds_add(data_interval_end, 7)}?unitGroup=metric&include=days&key={key}&contentType=csv')

    dados = pd.read_csv(URL)

    #print(dados.head()) também não queremos mais imprimir no terminal
    file_path = f'/home/luan-capella/Documentos/curso_airflow/airflowalura/semana={data_interval_end}/'
    #os.mkdir(file_path) criação da pasta que não precisa mais

    dados.to_csv(file_path + 'dados_brutos.csv')
    dados[['datetime', 'tempmin', 'temp', 'tempmax']].to_csv(file_path + 'temperaturas.csv')
    dados[['datetime', 'description', 'icon']].to_csv(file_path + 'condicoes.csv')


with DAG(
    'dados_climaticos', #nome na interface
    start_date=pendulum.datetime(2024, 9, 30, tz = "UTC"), #DAG vai começar na primeira segunda-feira do mês atual
    schedule_interval='0 0 * * 1', #min/ h /day /mounth /week-day(segunda-feira)
 ) as dag:
    
    tarefa_1 = BashOperator( #utilizado para executar comandos do terminal
        task_id = 'criar_pasta',#vamos criar o comando para criar uma pasta
        bash_command = 'mkdir -p "/home/luan-capella/Documentos/curso_airflow/airflowalura/semana={{data_interval_end.strftime("%Y-%m-%d")}}"'
    )
    
    tarefa_2 = PythonOperator(
        task_id = 'extrai_dados',
        python_callable = extrai_dados,
        op_kwargs = {'data_interval_end': '{{data_interval_end.strftime("%Y-%m-%d")}}'}#diconario de palavras chaves
    )
    
    tarefa_1 >> tarefa_2