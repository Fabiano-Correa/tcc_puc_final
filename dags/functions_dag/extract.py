from kafka import KafkaProducer
import pandas as pd
import os
import json
from shutil import move

def validate_file_kafka():
    file = read_file_and_filter()
    if file is None:
        return 'nvalidated'
    else:
        return 'validated'

def read_file_and_filter():
    input_directory='./datasets/'
    # Verifica se o diretório existe
    if not os.path.exists(input_directory):
        raise FileNotFoundError(f'Directory {input_directory} not found.')
    
    # Lista de extensões de arquivo suportadas
    supported_extensions = ['.txt','.csv']

    # Verifica se há arquivos com extensões suportadas no diretório
    file_list = [f for f in os.listdir(input_directory) 
                 if os.path.isfile(os.path.join(input_directory, f)) and any(f.lower().endswith(ext) for ext in supported_extensions)]
    
    if not file_list:
        return None
    else:
        print('Arquivos no diretório!!!')
        return os.path.join(input_directory, file_list[0])


def move_file_to_processed_directory(input_file):
    processed_directory = './datasets/processados/'

    # Cria o dubdiretório se não existir
    os.makedirs(processed_directory, exist_ok=True)

    # Monta o caminho de destino para o arquivo
    destination_file = os.path.join(processed_directory, os.path.basename(input_file))    

    # Move o arquivo para o subdiretório "processados"
    move(input_file, destination_file)

def process_file():
    input_directory='./datasets/'

    # Lista de extensões de arquivo suportadas
    supported_extensions = ['.txt','.csv']

    # Verifica se há arquivos com extensões suportadas no diretório
    file_list = [f for f in os.listdir(input_directory) 
                 if os.path.isfile(os.path.join(input_directory, f)) and any(f.lower().endswith(ext) for ext in supported_extensions)]
    
    # Assume que o primeiro arquivo encontrado é o que será processado
    input_file = os.path.join(input_directory, file_list[0])

    # leitura do arquivo
    file = pd.read_csv(input_file, sep=';', encoding='latin-1')
    
    # Move o arquivo para o subdiretório "processados"
    move_file_to_processed_directory(input_file)
    # Retorna os valores filtrados
    
    read_and_send_kafka(file)


def read_and_send_kafka(arq):
    kafka_bootstrap_servers = 'kafka:9092' # aqui eu coloco o nome interno do kafka para que o airflow consiga localizar o kafka para envio dos dados
    kafka_topic='tcc_raw'

    # converter Dataframe para formato JSON
    records = arq.to_dict(orient='records')

    # Conf do produtor Kafka
    kafka_producer_config = {
        'bootstrap_servers': kafka_bootstrap_servers,
        'value_serializer': lambda v: json.dumps(v).encode('utf-8')
    } 
    # inicializando o produtor
    producer = KafkaProducer(**kafka_producer_config)

    # Aguardar a confirmação de que todos os registros foram enviados
    for record in records:
        # converter o registro para uma string JSON
        # json_record = json.dumps(record) - Linha removida pois estava causando erro na conversão dos dados para o mongodb, visto que já existe uma conversão nas configurações acima.
        
        # Enviar para o tópico
        producer.send(kafka_topic, key=None, value=record)

    producer.flush()


