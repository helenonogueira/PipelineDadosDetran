import pandas as pd
import boto3
import io
import logging
import pymysql
import psutil

# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def sanitize_column_name(name):
    """Sanitiza os nomes das colunas para serem compatíveis com o MySQL."""
    return name.replace(" ", "_").replace("-", "_")

def processar_camada_silver():
    try:
        # Logar uso inicial de memória
        logger.info(f"Uso de memória inicial: {psutil.virtual_memory().percent}%")

        # Conectar ao MinIO
        logger.info("Conectando ao MinIO para baixar o arquivo Parquet...")
        s3_client = boto3.client(
            's3',
            endpoint_url="http://minio:9000",
            aws_access_key_id="minioadmin",
            aws_secret_access_key="minio@1234!"
        )

        # Download do arquivo Parquet
        bucket_name = "bronze"
        object_key = "data.parquet"
        logger.info(f"Baixando o arquivo {object_key} do bucket {bucket_name}...")
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        parquet_data = response['Body'].read()

        # Carregar o arquivo Parquet para um DataFrame
        df = pd.read_parquet(io.BytesIO(parquet_data))
        logger.info(f"Arquivo Parquet baixado e carregado com sucesso. Dimensões: {df.shape}")

        # Sanitizar nomes das colunas
        df.columns = [sanitize_column_name(col) for col in df.columns]

        # Processar e inserir dados em chunks
        chunk_size = 50000
        logger.info(f"Iniciando processamento em chunks de {chunk_size} linhas...")

        # Configurar conexão com o MySQL
        logger.info("Conectando ao banco de dados MySQL...")
        connection = pymysql.connect(
            host="mysql-airflow",
            user="airflow_user",
            password="airflow_password",
            database="airflow",
            port=3306
        )

        cursor = connection.cursor()

        # Criar tabela dinamicamente
        logger.info("Criando tabela dinamicamente no banco de dados...")
        column_types = []
        for column, dtype in zip(df.columns, df.dtypes):
            if pd.api.types.is_integer_dtype(dtype):
                sql_type = "INT"
            elif pd.api.types.is_float_dtype(dtype):
                sql_type = "DOUBLE"
            elif pd.api.types.is_datetime64_any_dtype(dtype):
                sql_type = "DATETIME"
            elif pd.api.types.is_object_dtype(dtype):
                sql_type = "TEXT"
            else:
                sql_type = "TEXT"  # Tipo genérico para tipos desconhecidos
            column_types.append(f"`{column}` {sql_type} NULL")

        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS acidentes_silver (
            {', '.join(column_types)}
        );
        """
        cursor.execute(create_table_query)
        connection.commit()
        logger.info("Tabela acidentes_silver criada/verificada com sucesso.")

        # Preparar para escrita incremental no MinIO
        silver_bucket_name = "silver"
        silver_object_key = "data_silver_final.parquet"
        buffer = io.BytesIO()

        # Processar os chunks
        for start in range(0, len(df), chunk_size):
            end = start + chunk_size
            chunk = df.iloc[start:end]

            # Tratar valores NaN no chunk
            logger.info(f"Processando chunk de linhas {start} a {end}...")
            chunk = chunk.where(pd.notnull(chunk), None)

            # Converter o chunk para uma lista de tuplas garantindo valores None
            rows = [tuple(None if pd.isna(val) else val for val in row) for row in chunk.to_numpy()]
            columns = ",".join([f"`{col}`" for col in chunk.columns])

            insert_query = f"INSERT INTO acidentes_silver ({columns}) VALUES ({','.join(['%s'] * len(chunk.columns))})"
            cursor.executemany(insert_query, rows)
            connection.commit()
            logger.info(f"Chunk {start // chunk_size + 1} inserido com sucesso!")

            # Adicionar chunk ao arquivo Parquet
            chunk.to_parquet(buffer, engine='pyarrow', index=False)

        buffer.seek(0)
        s3_client.put_object(Bucket=silver_bucket_name, Key=silver_object_key, Body=buffer.getvalue())
        logger.info(f"Arquivo final tratado salvo no bucket {silver_bucket_name} com o nome {silver_object_key}.")

        logger.info("Todos os dados foram inseridos na tabela acidentes_silver com sucesso!")

        cursor.close()
        connection.close()

        # Logar uso final de memória
        logger.info(f"Uso de memória final: {psutil.virtual_memory().percent}%")

    except Exception as e:
        logger.error(f"Erro ao processar a camada Silver: {e}")
        raise

if __name__ == "__main__":
    processar_camada_silver()
