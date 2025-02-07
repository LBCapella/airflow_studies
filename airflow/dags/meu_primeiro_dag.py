from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash_operator import BashOperator

with DAG(
    'meu_primeiro_dag', #nome na interface
    start_date=days_ago(2), #data de inicio da execução um dia antes do dia de hoje
    schedule_interval='@daily', #intervalo de agendamento   
 ) as dag:
    
    tarefa_1 = EmptyOperator(task_id= 'tarefa-1')#só serão executadas no airflow mas não fazem nada
    tarefa_2 = EmptyOperator(task_id= 'tarefa-2')
    tarefa_3 = EmptyOperator(task_id= 'tarefa-3')
    tarefa_4 = BashOperator( #utilizado para executar comandos do terminal
        task_id = 'criar_pasta',#vamos criar o comando para criar uma pasta
        bash_command = 'mkdir -p "/home/luan-capella/Documentos/curso_airflow/airflowalura/pasta={{data_interval_end}}"'
    )
    
    tarefa_1 >> [tarefa_2,tarefa_3] #aqui executa a tarefa 2 e 3 em paralelo
    tarefa_3 >> tarefa_4