from os import listdir
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

BASE_SQL_PATH = "/home/airflow/gcs/plugins/sql/"
LAYERS = ["DRE","DSG", "DWH", "DTS"]

def get_sql_directory(nom_dag):
    return BASE_SQL_PATH + nom_dag + "/"

def get_layer_directory(nom_dag, layer):
    return get_sql_directory(nom_dag) + layer + "/"

def get_layers_directories(nom_dag):
    directories = []
    for layer in LAYERS:
        directories.append(get_layer_directory(nom_dag, layer))
    return directories

def create_bq_task(file):
    run_bq_sql = BigQueryOperator (
        task_id = str(file),
        bigquery_conn_id = 'bigquery_conn_id',
        sql = file,
        use_legacy_sql=False)
    return run_bq_sql

def create_tasks(nom_dag):
    tasks_count = -1
    for layer in LAYERS:
        path = get_layer_directory(nom_dag, layer)
        for file in listdir(path):
            if file.endswith(".sql"):
                tasks_count = tasks_count + 1
                if tasks_count == 0:
                    previous_task = create_bq_task(file)
                else:
                    task = create_bq_task(file)
                    task.set_upstream(previous_task)
                    previous_task = task
