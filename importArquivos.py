import pandas as pd
import numpy as bo
import logging
import time
from memory_profiler import memory_usage

logging.basicConfig(filename='import_log.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def log_import(status, file_path, error=None):
    if status == "sucesso":
        logging.info(f"Arquivo {file_path} importado com sucesso.")
    elif status == "erro":
        logging.error(f"Erro ao importar o arquivo {file_path}: {error}")


def import_file(file_path):
    file_extension = file_path.split('.')[-1].lower()

    try:
        if file_extension == 'csv':
            df = pd.read_csv(file_path)
        elif file_extension == 'json':
            df = pd.read_json(file_path)
        elif file_extension in ['xlsx', 'xls']:
            df = pd.read_excel(file_path)
        elif file_extension == 'tsv':
            df = pd.read_csv(file_path, sep='\t')
        else:
            raise ValueError("Extensão de arquivo não suportada.")


        log_import("sucesso", file_path)
        return df

    except Exception as e:

        log_import("erro", file_path, error=str(e))
        raise e
    

def measure_import_performance(file_path):
    start_time = time.time()
    mem_usage_start = memory_usage()[0]
    try:
        df = import_file(file_path)
        mem_usage_end = memory_usage()[0]
        end_time = time.time()
       
        tempo_importacao = end_time - start_time
        memoria_utilizada = mem_usage_end - mem_usage_start

        print(f"Tempo de importação: {tempo_importacao:.2f} segundos")
        print(f"Memória utilizada: {memoria_utilizada:.2f} MB")

        logging.info(f"Desempenho da importação para {file_path}: "
                     f"Tempo de importação = {tempo_importacao:.2f} segundos, "
                     f"Memória utilizada = {memoria_utilizada:.2f} MB") 
        return df
    
    except Exception as e:
        raise e


file_path = r'C:\Users\iagom\Documents\Onearth\customers-500000.csv'
df = measure_import_performance(file_path)
df.head()