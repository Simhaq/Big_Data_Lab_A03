# Big Data Lab - Assignment 2 : Task1 Done By Simhaq M - ED19B031
import os
import shutil
import random
import argparse

from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from bs4 import BeautifulSoup

from zipfile import ZipFile 

# To parse the arguments through command line
parser = argparse.ArgumentParser(                                                        
                    prog='Big Data Lab Assignment2 Task1',
                    description='Airflow pipeline to Download the climate data given a year',)
parser.add_argument('--year', type = str , default = '2000', help = 'Year of climate data(1901-2024)')
parser.add_argument('--nfiles', type = int , default = 5, help = 'Number of files to selected')
parser.add_argument('--save_dir', type = str , default = '~/Documents', help = 'Path to save the archive')
args = parser.parse_args()

with DAG(dag_id="pipeline1", start_date=datetime(2024, 3, 10)) as dag:
    
    cwd = os.getcwd()
    year = args.year
    url = f'https://www.ncei.noaa.gov/data/local-climatological-data/access/{year}'

    def extract_files():
        # Extracts list of files(urls) in the html page and stores it in a text file 
        with open(year,'r') as f:
            page = f.read()
        f.close()

        # Reading the html file and extracting the csv files present in it
        soup = BeautifulSoup(page, 'html.parser')
        files = [url + '/' + node.get('href') for node in soup.find_all('a') if node.get('href').endswith('csv')]

        # Randomly selecting files
        n_files = args.nfiles
        n_files = min(n_files,len(files))
        files = random.sample(files, n_files)

        # Writing the urls of csv files into a text file
        with open('url_list.txt','w') as f:
            for file in files:
                f.write(file+'\n')
        f.close()

    def zipping_files():
        # Zips the csv files into climate_data_{year}.zip and deletes the unwanted files
        with ZipFile(f"climate_data_{year}.zip",'w') as zip_file:
            files = os.listdir(os.path.join(cwd,'Data'))
            for f in files:
                zip_file.write(f'Data/{f}',arcname = f)
        zip_file.close()

        # Deleting the unwanted files
        os.remove(f'{cwd}/{year}')
        os.remove(os.path.join(cwd,"url_list.txt"))
        shutil.rmtree(f'{cwd}/Data')
        
    
    fetch_page = BashOperator(task_id="Fetch_page", bash_command=f"wget {url} -P {cwd}")

    select_files = PythonOperator(task_id="Select_Files", python_callable = extract_files)

    fetch_files = BashOperator(task_id="Fetch_Files", bash_command=f"wget --input-file {cwd}/url_list.txt -P {cwd}/Data")

    zip_files = PythonOperator(task_id = "Zip_Files", python_callable = zipping_files)

    move_file = BashOperator(task_id="Move_Zip", bash_command=f"mv {cwd}/climate_data_{year}.zip {args.save_dir}")

    # Set dependencies between tasks
    fetch_page >> select_files >> fetch_files >> zip_files >> move_file

if __name__ == '__main__':
    dag.test()