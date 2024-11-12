import os
from botocore.exceptions import NoCredentialsError
import boto3

# Configurações do bucket e da pasta
bucket_name = 'ori-gcastro-ifood-poc'
pasta_local = 'parquets_origem'
pasta_no_bucket = 'taxi_roadtrip'  # Opcional: define um diretório dentro do bucket

# Cria uma sessão do cliente S3
s3 = boto3.client('s3')

def enviar_parquet_para_s3(pasta_local, bucket_name, pasta_no_bucket=''):
    for root, _, files in os.walk(pasta_local):
        for file in files:
            if file.endswith('.parquet'):
                caminho_arquivo = os.path.join(root, file)
                caminho_no_bucket = os.path.join(pasta_no_bucket, file)
                
                try:
                    # Envia o arquivo para o bucket
                    s3.upload_file(caminho_arquivo, bucket_name, caminho_no_bucket)
                    print(f'Arquivo {file} enviado com sucesso para {bucket_name}/{caminho_no_bucket}')
                except NoCredentialsError:
                    print('Credenciais AWS não encontradas.')
                except Exception as e:
                    print(f'Erro ao enviar {file}: {e}')

# Executa a função
enviar_parquet_para_s3(pasta_local, bucket_name, pasta_no_bucket)