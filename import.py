import pandas as pd
import logging
import time
from memory_profiler import memory_usage
import os

logging.basicConfig(filename='import_log.log', level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def log_import(status, file_path, error=None):
    if status == "sucesso":
        logging.info(f"Arquivo {file_path} importado com sucesso.")
    elif status == "erro":
        logging.error(f"Erro ao importar o arquivo {file_path}: {error}")
    else:
        logging.debug(f"Status desconhecido para o arquivo {file_path}: {status}")


def validar_tipos_dados(df, tipos_esperados):
    erros = []
    for index, row in df.iterrows():
        for coluna, tipo in tipos_esperados.items():
            if coluna not in df.columns:
                logging.warning(f"Coluna '{coluna}' não encontrada no arquivo importado.")
                continue
            valor = row[coluna]
            if not isinstance(valor, tipo):
                erros.append((index + 1, coluna, valor, type(valor).__name__))
                logging.error(f"Erro de tipo na linha {index + 1}, coluna '{coluna}': valor '{valor}' é {type(valor).__name__}, esperado {tipo.__name__}")
    return erros


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

def salvar_dataframe(df, file_path):
    output_dir = 'arquivo_processado'
    os.makedirs(output_dir, exist_ok=True)
    extensao_entrada = os.path.splitext(file_path)[-1].lower()
    
    if extensao_entrada == '.csv':
        output_path = os.path.join(output_dir, os.path.splitext(os.path.basename(file_path))[0] + "_processed.csv")
        df.to_csv(output_path, index=False)
    elif extensao_entrada == '.json':
        output_path = os.path.join(output_dir, os.path.splitext(os.path.basename(file_path))[0] + "_processed.json")
        df.to_json(output_path, orient='records', lines=True)
    elif extensao_entrada in ['.xlsx', '.xls']:
        output_path = os.path.join(output_dir, os.path.splitext(os.path.basename(file_path))[0] + "_processed.xlsx")
        df.to_excel(output_path, index=False)
    elif extensao_entrada == '.tsv':
        output_path = os.path.join(output_dir, os.path.splitext(os.path.basename(file_path))[0] + "_processed.tsv")
        df.to_csv(output_path, sep='\t', index=False)
    else:
        raise ValueError("Extensão de arquivo não suportada para exportação.")
    
    logging.info(f"DataFrame salvo em {output_path}")
    print(f"DataFrame salvo em: {output_path}")


def measure_import_performance(file_path, tipos_esperados):
    start_time = time.time()
    mem_usage_start = memory_usage()[0]
    try:
        df = import_file(file_path)
        erros = validar_tipos_dados(df, tipos_esperados)
        
        if erros:
            logging.warning(f"Foram encontrados {len(erros)} erros de tipo no arquivo {file_path}.")
        else:
            logging.info(f"Nao foram encontrados erros de tipo no arquivo {file_path}.")
        
        mem_usage_end = memory_usage()[0]
        end_time = time.time()
        tempo_importacao = end_time - start_time
        memoria_utilizada = mem_usage_end - mem_usage_start

        print(f"Tempo de importacao: {tempo_importacao:.2f} segundos")
        print(f"Memória utilizada: {memoria_utilizada:.2f} MB")

        logging.info(f"Desempenho da importacao para {file_path}: "
                     f"Tempo de importacao = {tempo_importacao:.2f} segundos, "
                     f"Memória utilizada = {memoria_utilizada:.2f} MB") 
        
        salvar_dataframe(df, file_path)
        return df
    
    except Exception as e:
        raise e


tipos_esperados = {
    "Index": int,
    "Customer Id": str,
    "First Name": str,
    "Last Name": str,
    "Company": str,
    "City": str,
    "Country": str,
    "Phone 1": str,
    "Phone 2": str,
    "Email": str,
    "Subscription Date": str,
    "Website": str
}

file_path = r'C:\Users\iagom\Documents\Onearth\data\data.json'

df = measure_import_performance(file_path, tipos_esperados)

print(df.head(10))
