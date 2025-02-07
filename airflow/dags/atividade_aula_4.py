from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

def boas_vindas():
    print("Boas vindas ao Airflow!")

with DAG(
    'atividade_aula_4', #nome na interface
    start_date=days_ago(1), #data de inicio da execução um dia antes do dia de hoje
    schedule_interval='@daily', #intervalo de agendamento   
 ) as dag:
    
    tarefa_1 = EmptyOperator(task_id= 'tarefa-1')#só serão executadas no airflow mas não fazem nada
    
    tarefa_2 = PythonOperator(
        task_id = 'tarefa_2',
        python_callable=boas_vindas
    )
    
    tarefa_1 >> tarefa_2