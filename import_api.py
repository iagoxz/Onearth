from fastapi import FastAPI, UploadFile, HTTPException
import pandas as pd
import logging
import os
from pydantic import BaseModel
from typing import Optional

import time
from memory_profiler import memory_usage

app = FastAPI()


logging.basicConfig(filename='import_log.log', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')


class TipoEsperado(BaseModel):
    Ano: int
    Mês: Optional[str]
    Escopo: int 
    Categoria: str
    Subcategoria: str
    Unidade_Operacional: str
    Setor: Optional[str]
    Atividade: str
    Insumo_ou_produto: str
    Fossil_ou_biogenico: str
    Teor_de_carbono: float
    Quantidade_do_insumo_ou_produto: float
    Unidade_de_medida: str
    Possui_controle_operacional: str
    Tipo_de_dado_coletado: str
    Rastreabilidade: str
    Observacoes: Optional[str]


def log_import(status, file_path, error=None):
    if status == "sucesso":
        logging.info(f"Arquivo {file_path} importado com sucesso.")
    elif status == "erro":
        logging.error(f"Erro ao importar o arquivo {file_path}: {error}")

def validar_tipos_dados(df, model_class):
    erros = []
    TipoEsperado = model_class.__annotations__
    
    for index, row in df.iterrows():
        for coluna, tipo in TipoEsperado.items():
            if coluna not in df.columns:
                logging.warning(f"Coluna '{coluna}' não encontrada no arquivo importado.")
                erros.append((index + 1, coluna, None, "Coluna ausente"))
                continue

            valor = row[coluna]

            if valor is None:
                continue
          
            if not isinstance(valor, tipo):
                erros.append((index + 1, coluna, valor, type(valor).__name__))
                logging.error(f"Erro de tipo na linha {index + 1}, coluna '{coluna}': valor '{valor}' é {type(valor).__name__}, esperado {tipo.__name__}")
    
    return erros

def import_file(file: UploadFile):
    file_extension = file.filename.split('.')[-1].lower()
    try:
        if file_extension == 'csv':
            df = pd.read_csv(file.file)
        elif file_extension == 'json':
            df = pd.read_json(file.file)
        elif file_extension == 'xls':
            df = pd.read_excel(file.file, engine='xlrd')
        elif file_extension == 'xlsx':
            df = pd.read_excel(file.file, engine='openpyxl')
        elif file_extension == 'tsv':
            df = pd.read_csv(file.file, sep='\t')
        else:
            raise ValueError("Extensão de arquivo não suportada.")
        
        log_import("sucesso", file.filename)
        return df
    except Exception as e:
        log_import("erro", file.filename, error=str(e))
        raise e

def salvar_dataframe(df, file_name):
    output_dir = 'arquivo_processado'
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, os.path.splitext(file_name)[0] + "_processed.xlsx")
    df.to_csv(output_path, index=False)
    logging.info(f"DataFrame salvo em {output_path}")
    return output_path

def measure_import_performance(file: UploadFile):
    start_time = time.time()
    mem_usage_start = memory_usage()[0]
    
    try:
        df = import_file(file)
        df = df.dropna()
        erros = validar_tipos_dados(df, TipoEsperado)
        
        if erros:
            logging.warning(f"Foram encontrados {len(erros)} erros de tipo no arquivo {file.filename}.")
        else:
            logging.info(f"Nao foram encontrados erros de tipo no arquivo {file.filename}.")
        
        mem_usage_end = memory_usage()[0]
        end_time = time.time()
        tempo_importacao = end_time - start_time
        memoria_utilizada = mem_usage_end - mem_usage_start

        output_path = salvar_dataframe(df, file.filename)

        return {
            "tempo_importacao": f"{tempo_importacao:.2f} segundos",
            "memoria_utilizada": f"{memoria_utilizada:.2f} MB",
            "erros": erros,
            "caminho_arquivo": output_path
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



@app.post("/upload/")
async def upload_file(file: UploadFile):
    try:
        result = measure_import_performance(file)
        return {"mensagem": "Arquivo processado com sucesso!", "detalhes": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao processar o arquivo: {str(e)}")

@app.get("/logs/")
def get_logs():
    try:
        with open("import_log.log", "r") as log_file:
            logs = log_file.readlines()
        return {"logs": logs}
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Log file not found.")

