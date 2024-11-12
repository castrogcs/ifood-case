import boto3
import pandas as pd
from io import BytesIO
from datetime import datetime
import os

# Configurações dos buckets e caminhos
bucket_origem = 'ori-gcastro-ifood-poc'
bucket_destino = 'silver-gcastro-ifood-poc'
pasta_no_bucket_origem = 'taxi_roadtrip/'  # Caminho no bucket de origem, se houver
pasta_destino = 'silver'
colunas_desejadas = ['VendorID', 'passenger_count', 'total_amount', 'tpep_pickup_datetime', 'tpep_dropoff_datetime']

# Cria uma sessão S3
s3 = boto3.client('s3')

def ler_parquets_bucket(bucket_name, pasta):
    dataframes = []
    
    # Lista os arquivos .parquet no bucket de origem
    lista = s3.list_objects_v2(Bucket=bucket_name, Prefix=pasta)
    
    for obj in lista.get('Contents', []):
        if obj['Key'].endswith('.parquet'):
            # Baixa o arquivo .parquet do S3 para a memória
            parquet_obj = s3.get_object(Bucket=bucket_name, Key=obj['Key'])
            parquet_body = parquet_obj['Body'].read()
            
            # Lê o arquivo .parquet para um DataFrame
            parquet_df = pd.read_parquet(BytesIO(parquet_body))
            
            # Adiciona colunas faltantes para garantir que todas as colunas desejadas estejam presentes
            for coluna in colunas_desejadas:
                if coluna not in parquet_df.columns:
                    parquet_df[coluna] = pd.NA
            
            # Seleciona apenas as colunas desejadas
            parquet_df = parquet_df[colunas_desejadas]
            
            # Adiciona colunas com o nome do arquivo
            parquet_df['arquivo_origem'] = obj['Key']
            
            dataframes.append(parquet_df)
    
    # Concatena todos os DataFrames em um único DataFrame
    if dataframes:
        return pd.concat(dataframes, ignore_index=True)
    else:
        return pd.DataFrame(columns=colunas_desejadas + ['arquivo_origem'])

def salvar_parquet_localmente(dataframe, pasta_destino):
    """Salva o DataFrame consolidado em um arquivo .parquet em uma pasta local."""
    # Define o caminho do arquivo com estrutura incremental baseada na data e hora
    if not os.path.exists(pasta_destino):
        os.makedirs(pasta_destino)
    caminho_arquivo_local = os.path.join(pasta_destino, f'taxi_roadtrip.parquet')
    
    # Salva o DataFrame consolidado como .parquet localmente
    dataframe.to_parquet(caminho_arquivo_local, index=False)
    print(f'Arquivo consolidado salvo localmente em: {caminho_arquivo_local}')
    
    return caminho_arquivo_local

def enviar_parquet_para_s3(caminho_arquivo_local, bucket_name):
    """Envia um arquivo Parquet para um bucket S3 como um arquivo .parquet."""

    arquivo_destino = 'taxi_roadtrip.parquet'
    
    # Lê o arquivo .parquet salvo localmente e o envia para o bucket de destino
    with open(caminho_arquivo_local, 'rb') as file_data:
        s3.put_object(Bucket=bucket_name, Key=arquivo_destino, Body=file_data)
    print(f'Arquivo consolidado enviado com sucesso para {bucket_name}/{arquivo_destino}')

def main():
    # Lê e consolida os arquivos .parquet do bucket de origem
    df_consolidado = ler_parquets_bucket(bucket_origem, pasta_no_bucket_origem)
    
    if not df_consolidado.empty:
        # Salva o arquivo localmente
        caminho_arquivo_local = salvar_parquet_localmente(df_consolidado, pasta_destino)
        
        # Envia o arquivo salvo localmente para o bucket de destino
        enviar_parquet_para_s3(caminho_arquivo_local, bucket_destino)
    else:
        print("Nenhum arquivo .parquet encontrado no bucket de origem.")

if __name__ == "__main__":
    main()