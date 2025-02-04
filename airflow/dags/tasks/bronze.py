import pandas as pd
import boto3
import os
import logging
import traceback
import sys

# Configuração dos logs
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),  # Logs no console
        logging.FileHandler('/opt/airflow/logs/bronze_script.log')  # Logs em arquivo
    ]
)
logger = logging.getLogger(__name__)

def carregar_e_salvar_em_chunks(caminho_arquivo_csv, caminho_arquivo_parquet, chunksize=50000):
    """
    Função para processar um arquivo CSV em chunks, normalizar dados e salvá-lo como Parquet.
    """
    try:
        logger.info(f"Iniciando conversão do CSV para Parquet em chunks de {chunksize} linhas.")

        chunks = []
        for chunk in pd.read_csv(caminho_arquivo_csv, sep=';', encoding='latin1', chunksize=chunksize):
            logger.info(f"Lendo chunk com dimensões: {chunk.shape}")

            # Normalização e conversão de tipos
            if 'latitude' in chunk.columns and 'longitude' in chunk.columns:
                chunk['latitude'] = chunk['latitude'].astype(str).str.replace(',', '.').astype(float)
                chunk['longitude'] = chunk['longitude'].astype(str).str.replace(',', '.').astype(float)
                logger.info("Normalização das colunas latitude e longitude concluída neste chunk.")

            if 'feridos_leves' in chunk.columns:
                chunk['feridos_leves'] = chunk['feridos_leves'].fillna(0).astype(int)

            if 'feridos_graves' in chunk.columns:
                chunk['feridos_graves'] = chunk['feridos_graves'].fillna(0).astype(int)

            if 'mortos' in chunk.columns:
                chunk['mortos'] = chunk['mortos'].fillna(0).astype(int)

            if 'data_inversa' in chunk.columns:
                chunk['data_inversa'] = pd.to_datetime(chunk['data_inversa'], format='%Y-%m-%d', errors='coerce').dt.date

            if 'horario' in chunk.columns:
                chunk['horario'] = pd.to_datetime(chunk['horario'], format='%H:%M:%S', errors='coerce').dt.time

            chunks.append(chunk)

        # Concatenar todos os chunks
        df = pd.concat(chunks, ignore_index=True)
        logger.info(f"DataFrame final concatenado. Dimensões: {df.shape}")

        # Salvar o DataFrame como arquivo Parquet
        logger.info(f"Salvando o DataFrame no formato Parquet: {caminho_arquivo_parquet}")
        df.to_parquet(caminho_arquivo_parquet, index=False)
        logger.info("Conversão concluída com sucesso.")

    except Exception as e:
        logger.error(f"Erro ao converter o CSV para Parquet: {e}")
        logger.error(traceback.format_exc())
        raise

def enviar_para_minio(caminho_arquivo_parquet, bucket_name, endpoint_url, access_key, secret_key):
    """
    Função para enviar o arquivo Parquet ao MinIO.
    """
    try:
        logger.info(f"Iniciando envio do arquivo Parquet para o bucket {bucket_name} no MinIO.")

        # Configurar cliente MinIO
        minio_client = boto3.client(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key
        )

        # Verificar se o arquivo Parquet existe
        if not os.path.exists(caminho_arquivo_parquet):
            raise FileNotFoundError(f"Arquivo Parquet não encontrado: {caminho_arquivo_parquet}")

        # Preparar o arquivo para upload
        with open(caminho_arquivo_parquet, 'rb') as parquet_file:
            minio_client.put_object(
                Bucket=bucket_name,
                Key="data.parquet",
                Body=parquet_file
            )
        logger.info("Arquivo Parquet enviado para o MinIO com sucesso.")

    except Exception as e:
        logger.error(f"Erro ao enviar o arquivo Parquet para o MinIO: {e}")
        logger.error(traceback.format_exc())
        raise

def main():
    try:
        # Caminhos dos arquivos
        caminho_arquivo_csv = '/opt/airflow/data/acidentes_2024.csv'
        caminho_arquivo_parquet = '/opt/airflow/data/acidentes_2024.parquet'

        # Configurações do MinIO
        bucket_name = "bronze"
        endpoint_url = "http://minio:9000"
        access_key = "minioadmin"
        secret_key = "minio@1234!"

        # Processar o arquivo CSV e salvá-lo como Parquet
        carregar_e_salvar_em_chunks(caminho_arquivo_csv, caminho_arquivo_parquet)

        # Enviar o arquivo Parquet para o MinIO
        enviar_para_minio(caminho_arquivo_parquet, bucket_name, endpoint_url, access_key, secret_key)

        logger.info("Processo da camada Bronze concluído com sucesso!")

    except Exception as e:
        logger.error(f"Erro na execução principal: {e}")
        logger.error(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    main()
