# Big Data Lab - Assignment 2 : Task2 Done By Simhaq M - ED19B031

import os
import pandas as pd
import argparse

from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.sensors.filesystem import FileSensor

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

# To parse the arguments through command line
parser = argparse.ArgumentParser(                                                        
                    prog='Big Data Lab Assignment2 Task2',
                    description='Airflow pipeline to create visualizations from climate data',)
parser.add_argument('--data_path', type = str , default = '/home/simhaq/Documents/climate_data_2000.zip', help = 'Path to location where the archive is present')
parser.add_argument('--req_fields', type = str , nargs='+', default = ['HourlyWindSpeed'], help = 'List of required fields in the data')
args = parser.parse_args()


cwd = os.getcwd()

def read_csv(csv_path,req_fields):
    import pandas as pd
    # Loading the csv file into pandas dataframe
    df = pd.read_csv(csv_path)

    # Filtering the required fields
    req_fields.insert(0,'DATE')
    info = df[req_fields]

    # Handling different data types and missing values 
    for x in req_fields[1:]:
        info[x] = pd.to_numeric(info[x],errors = 'coerce')
    info.dropna(inplace = True)
    info = info.to_numpy()

    # Getting the required Tuple output
    lat = df['LATITUDE'].iloc[0]
    lon = df['LONGITUDE'].iloc[0]
    data = (lat,lon,info)
    return data

def Extract_csv(Fields):

    # Setting the beam options
    beam_options = PipelineOptions(
    runner='DirectRunner',
    project='Big-Data-Lab',
    job_name='climate-data-processing',
    temp_location='/tmp',
    direct_num_workers=6,
    direct_running_mode='multi_processing' #multi_threading | multi_processing | in_memory
    )

    list_of_files = [os.path.join(cwd+'/data',x) for x in os.listdir(cwd+'/data')]

    # Defining the beam pipeline
    p = beam.Pipeline(options = beam_options)
    
    req_fields = p | "Required Fields" >> beam.Create(Fields)
    extracted_data = (  p
                        | "Parse Files" >> beam.Create(list_of_files)
                        | "Read CSV" >> beam.Map(read_csv, req_fields = beam.pvalue.AsList(req_fields))
                        | beam.io.WriteToText(f'{cwd}/output')
                     )
    p.run()    
    
with DAG(dag_id="pipeline2", start_date=datetime(2024, 3, 8), schedule="0 0 * * *") as dag:
    
    data_path = args.data_path

    req_fields = args.req_fields

    # Waits for the archive files for 5 minutes, if files not present stops the pipeline
    wait_for_archive = FileSensor(task_id='Wait_for_Archive', filepath= data_path, poke_interval=10, timeout=300)

    # Bash Command to check for a valid archive, if yes then unzip the files in the archive 
    check_and_unzip_command = f"""
    if file {data_path} | grep -q 'Zip archive data'; then
        echo "Valid archive";
        unzip {data_path} -d {cwd}/data;
    else
        echo "Not a valid archive";
        exit 1;
    fi
    """
    
    check_and_unzip = BashOperator(task_id = 'Check_and_Unzip', bash_command = check_and_unzip_command)

    extract_and_filter_csv = PythonOperator(task_id = "Extract_and_filter_csv", python_callable = Extract_csv, op_args = [req_fields]) 

    compute_monthly_averages = DummyOperator(task_id = "Compute_monthly_averages")

    create_visualization = DummyOperator(task_id = "Create_Visualization")

    delete_files = BashOperator(task_id = 'Delete_Files',bash_command = f"rm -r {cwd}/data")

    

    wait_for_archive >> check_and_unzip >> extract_and_filter_csv >> compute_monthly_averages >> create_visualization >> delete_files

if __name__ == '__main__':
    dag.test()
    