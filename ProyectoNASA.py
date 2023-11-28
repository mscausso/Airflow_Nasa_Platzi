from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.sensors.filesystem import FileSensor
from datetime import datetime, date

default_args={"depends_on_past":True}

def _generate_platzi_data(**kwargs):
    import pandas as pd
    data = pd.DataFrame({"student": ["Maria Cruz", "Daniel Crema", "Elon Musk", "Karol Castrejon", "Freddy Vega", "Max Causso"],
                         "timestamp": [kwargs['logical_date'], kwargs['logical_date'], kwargs['logical_date'],
                                       kwargs['logical_date'], kwargs['logical_date'], kwargs['logical_date']]})
    data.to_csv(f"/tmp/platzi_data_{kwargs['ds_nodash']}.csv", header=True)

with DAG(dag_id="Proyecto_Nasa",
         description="Proyecto Platzi explora el espacio",
         schedule_interval="@daily",
         start_date=datetime(2023,11,25),
         end_date=datetime(2023,12,1),
         max_active_runs=1) as dag:
    
    tarea_nasa = BashOperator(task_id="t_nasa",
                          bash_command="sleep 20 && echo 'OK conectado a la NASA' >/tmp/response_{{ds_nodash}}.txt")
    
    tarea_datos_nasa = BashOperator(task_id="t_datos_nasa",
                                    bash_command="ls /tmp && head /tmp/response_{{ds_nodash}}.txt")
    
    tarea_SpaceX = BashOperator(task_id="t_spacex_api",
                                bash_command="curl -o /tmp/history.json -L 'https://api.spacexdata.com/v4/history'")
    
    tarea_Python = PythonOperator(task_id="Satellite_response",
                                  python_callable=_generate_platzi_data)
    
    tarea_file_sensor = FileSensor(task_id="esperar_archivo",
                                   filepath="/tmp/platzi_data_{{ds_nodash}}.csv")
    
    tarea_ver_csv = BashOperator(task_id="ver_csv",
                                 bash_command="ls /tmp && head /tmp/platzi_data_{{ds_nodash}}.csv")
    
    tarea_enviar_mensaje = BashOperator(task_id="enviar_mensaje",
                                        bash_command="echo '{{ds}}: Los datos fueron cargados'")
    
    tarea_nasa >>  tarea_datos_nasa >> tarea_SpaceX >> tarea_Python >> tarea_file_sensor >> tarea_ver_csv >> tarea_enviar_mensaje
    

                                              

    