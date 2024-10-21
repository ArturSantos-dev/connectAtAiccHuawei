import requests
import datetime
import hmac
import hashlib
import base64
import os

from scipy.datasets import download_all

# Suas credenciais do OBS
access_key = 'your-access-key-id'
secret_key = 'your-secret-access-key'
region = 'your-region'  # ex: sa-saopaulo-1 para São Paulo
bucket_name = 'your-bucket-name'
folder_prefix = 'your-folder/'  # Caminho da pasta no bucket

# URL para listar os arquivos na pasta
url = f'https://{bucket_name}.obs.{region}.myhuaweicloud.com/?prefix={folder_prefix}'

# Função para gerar a assinatura HMAC-SHA1 e codificar em base64
def generate_authorization(access_key, secret_key, date, bucket_name, object_key=""):
    string_to_sign = f"GET\n\n\n{date}\n/{bucket_name}/{object_key}"
    signature = hmac.new(secret_key.encode(), string_to_sign.encode(), hashlib.sha1).digest()
    signature_b64 = base64.b64encode(signature).decode()
    return f"OBS {access_key}:{signature_b64}"

# Gerar a data no formato correto
date = datetime.datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')

# Gerar a autorização HMAC-SHA1 para a listagem
authorization = generate_authorization(access_key, secret_key, date, bucket_name)

# Cabeçalhos da requisição para listar os arquivos
headers = {
    'Host': f'{bucket_name}.obs.{region}.myhuaweicloud.com',
    'Date': date,
    'Authorization': authorization,
}

# Fazer a requisição GET para listar os arquivos na pasta
response = requests.get(url, headers=headers)

# Verificar se a listagem foi bem-sucedida
if response.status_code == 200:
    # Parse da resposta XML para encontrar os arquivos
    import xml.etree.ElementTree as ET
    root = ET.fromstring(response.text)
    
    # Iterar sobre os objetos retornados
    for content in root.findall('.//{http://obs.myhuaweicloud.com/doc/2015-06-30/}Contents'):
        file_key = content.find('{http://obs.myhuaweicloud.com/doc/2015-06-30/}Key').text
        if file_key.endswith('.csv') or file_key.endswith('.xls'):
            # Baixar o arquivo se for .csv ou .xls
            print(f"Baixando arquivo: {file_key}")
            download_all(bucket_name, file_key)
else:
    print(f"Erro {response.status_code}: {response.text}")

# Função para baixar o arquivo
def download_file(bucket_name, file_key):
    # URL para download do arquivo
    file_url = f'https://{bucket_name}.obs.{region}.myhuaweicloud.com/{file_key}'

    # Gerar a autorização HMAC-SHA1 para o download
    date = datetime.datetime.now(datetime).strftime('%a, %d %b %Y %H:%M:%S GMT')
    authorization = generate_authorization(access_key, secret_key, date, bucket_name, file_key)

    # Cabeçalhos para o download
    headers = {
        'Host': f'{bucket_name}.obs.{region}.myhuaweicloud.com',
        'Date': date,
        'Authorization': authorization,
    }

    # Fazer a requisição GET para baixar o arquivo
    response = requests.get(file_url, headers=headers, stream=True)

    # Verificar se o download foi bem-sucedido
    if response.status_code == 200:
        # Criar um arquivo local com o nome correto
        local_filename = os.path.basename(file_key)  # Extrai o nome do arquivo
        with open(local_filename, 'wb') as f:
            # Copiar o conteúdo da resposta para o arquivo local
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:  # Filtra chunks vazios
                    f.write(chunk)
        print(f"Download concluído: {local_filename}")
    else:
        print(f"Erro ao baixar {file_key}: {response.status_code} - {response.text}")
