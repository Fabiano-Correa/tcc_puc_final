import requests
from pymongo import MongoClient
import mysql.connector

def get_ibge():
    ibg_api_url = 'https://servicodados.ibge.gov.br/api/v1/localidades/estados/33/distritos'
    response = requests.get(ibg_api_url)

    # Verificar se a requisição foi bem sucedida(Status Code = 200)
    if response.status_code==200:
        ibge_data = response.json()
        set_localidades(ibge_data)
    else:
        print(f'Erro ao acessar a API do IBGE. Código de Status: {response.status_code}')
        ibge_data = None

def set_localidades(data): # nesse método não utilizarei nenhum tópico kafka
    # configurações mongo
    mongodb_uri='mongodb://root:root@mongodb/'
    mongodb_database='tcc'
    mongodb_collection='tcc_localidades'

    # Conectando ao MongoDB
    client = MongoClient(mongodb_uri)
    db = client[mongodb_database]
    collection = db[mongodb_collection]

    try:
        # Verificar se os novos dados são diferentes dos existentes
        existing_data = list(collection.find())
        if not existing_data or existing_data != data:
            # Remover dados existentes
            collection.delete_many({})

            # Inserir novos dados
            collection.insert_many(data)
            print('Dados inseridos no MongoDB com sucesso.')

        else:
            print('Dados já existentes e não houve alteração.')

    except Exception as e:
        print(f"Ocorreu um erro ao inserir dados no MongoDB: {e}")

    finally:
        client.close()


def save_local_to_mysql():
    mongodb_uri = 'mongodb://root:root@mongodb/'
    mongodb_database = 'tcc'
    collection = 'tcc_localidades'

    client = MongoClient(mongodb_uri)
    db = client[mongodb_database]
    collection = db[collection]

    # passar os dados para variáveis de ambiente
    mysql_host = 'mysql'
    mysql_user = 'root'
    mysql_password = 'root'
    mysql_database = 'tcc_final_data'

    mysql_connection = mysql.connector.connect(
        host=mysql_host,
        user = mysql_user,
        password = mysql_password,
        database=mysql_database
    )
    mysql_cursor = mysql_connection.cursor()

    try:
        mongo_data = list(collection.find())        

        for record in mongo_data:
            cod_cid = record['id']
            mesorregiao = record['municipio']['microrregiao']['mesorregiao']['nome']
            uf = record['municipio']['microrregiao']['mesorregiao']['UF']['sigla']
            estado = record['municipio']['microrregiao']['mesorregiao']['UF']['nome']

            query = 'insert into localidades (cod_cid, mesorregiao,uf,estado) values (%s, %s, %s, %s)'
            values = (cod_cid, mesorregiao, uf, estado)
            print(query,values)
            mysql_cursor.execute(query, values)
    
        mysql_connection.commit()
        print("Dados inseridos com sucesso.")
    
    except Exception as e:
        print(f'Ocorreu um erro ao inserir os dados de localidade no MySQL.')

    finally:
        client.close()
        mysql_cursor.close()
        mysql_connection.close()