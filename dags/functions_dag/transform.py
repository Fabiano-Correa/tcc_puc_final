import json
import pandas as pd
from pymongo import MongoClient
from kafka import KafkaProducer
import mysql.connector

def remove_duplicates():
   # configurações mongo
   mongodb_uri='mongodb://root:root@mongodb/'
   mongodb_database='tcc'
   mongodb_collection='tcc_trusted'

   # Conectando ao MongoDB
   client = MongoClient(mongodb_uri)
   db = client[mongodb_database]
   collection = db[mongodb_collection]

   try:
      # Criar um DataFrame a partir dos documentos MongoDB
      df = pd.DataFrame(list(collection.find()))

      # Especificar as colunas usadas para identificar duplicatas
      columns_to_check = ["ano", "mes", "codigo_municipio",'tipo_de_crime','qtd_registros']

      # Encontrar e manter apenas a primeira ocorrência de cada duplicata
      df_no_duplicates = df.drop_duplicates(subset=columns_to_check, keep="first")

      # Remover todos os documentos da coleção
      collection.delete_many({})

      # Inserir os documentos não duplicados de volta na coleção
      collection.insert_many(df_no_duplicates.to_dict(orient="records"))

      print("Duplicatas removidas com sucesso.")

   except Exception as e:
      print(f"Ocorreu um erro: {e}")

   finally:
      # Fechar a conexão com o MongoDB
      client.close()

def map_tipo_de_crime(tipo_de_crime)      :
    # Dicionário de mapeamento
    mapping_dict = {
        'hom_doloso': 'Homicídio Doloso',
        'lesao_corp_morte': 'Lesão Corporal seguida de Morte',
        'latrocinio': 'Latrocínio',
        'hom_por_interv_policial': 'Homicídio por Invervenção Policial',
        'letalidade_violenta': 'Letalidade Violenta',
        'tentat_hom': 'Tentativa de Homicídio',
        'estupro': 'Estupro',
        'total_roubos': 'Total de Roubos',
        'total_furtos': 'Total de Furtos',
        'sequestro': 'Sequestro',
        'pessoas_desaparecidas': 'Pessoas Desaparecidas'
    }
    # Retorna o valor mapeado ou o valor original se não estiver no dicionário
    return mapping_dict.get(tipo_de_crime, tipo_de_crime)
   

def reestruturar_dados(df):
    # Especificar as colunas-chave e as colunas a serem transformadas em linhas
    key_columns = ['fmun_cod', 'fmun', 'ano', 'mes']
    value_columns = ['hom_doloso', 'lesao_corp_morte', 'latrocinio','hom_por_interv_policial','letalidade_violenta','tentat_hom','estupro','total_roubos','total_furtos','sequestro','pessoas_desaparecidas']

    # Usar a função melt para reestruturar o DataFrame
    df_reestruturado = pd.melt(df, id_vars=key_columns, value_vars=value_columns, var_name='tipo_de_crime', value_name='qtd_registros')

    # Remover colunas duplicadas
    df_reestruturado = df_reestruturado.drop_duplicates()

    return df_reestruturado

def raw_to_trusted():
   # configurações mongo
   mongodb_uri = 'mongodb://root:root@mongodb/'
   mongodb_database = 'tcc'
   mongodb_collection = 'tcc_raw'

   # Configurações Kafka
   kafka_bootstrap_servers = 'kafka:9092'
   kafka_topic = 'tcc_trusted'

   # Configurações produtor Kafka
   kafka_producer_config = {
        'bootstrap_servers': kafka_bootstrap_servers,
        'value_serializer': lambda v: json.dumps(v).encode('utf-8')
    } 

   # Conectando ao MongoDB
   client = MongoClient(mongodb_uri)
   db = client[mongodb_database]
   collection = db[mongodb_collection]

   try:
       # Criar um DataFrame a partir dos documentos MongoDB
       df = pd.DataFrame(list(collection.find()))

       # Reestruturar os dados
       df_reestruturado = reestruturar_dados(df)

       # Mapear os tipos de crime
       df_reestruturado['tipo_de_crime'] = df_reestruturado['tipo_de_crime'].apply(map_tipo_de_crime)

       # Conectar ao Kafka
       producer = KafkaProducer(**kafka_producer_config)

       # Iterar sobre as linhas do DataFrame reestruturado
       for _, row in df_reestruturado.iterrows():
           transformed_data = {
               'codigo_municipio': row['fmun_cod'],
               'municipio': row['fmun'],
               'ano': row['ano'],
               'mes': row['mes'],
               'tipo_de_crime': row['tipo_de_crime'],
               'qtd_registros': row['qtd_registros']
           }

           # Enviando para o Kafka
           producer.send(kafka_topic, key=None, value=transformed_data)

       print("Dados enviados para o Kafka com sucesso.")

   except Exception as e:
       print(f"Ocorreu um erro: {e}")

   finally:
       # Fechar conexões
       producer.flush()
       producer.close()
       client.close()

def trusted_to_refined():
   # configurações mongo
   mongodb_uri='mongodb://root:root@mongodb/'
   mongodb_database='tcc'
   mongodb_collection='tcc_trusted'

   # Configurações Kafka
   kafka_bootstrap_servers = 'kafka:9092' # aqui eu coloco o nome interno do kafka para que o airflow consiga localizar o kafka para envio dos dados
   kafka_topic='tcc_refined'

   # Configurações produtor Kafka
   kafka_producer_config = {
        'bootstrap_servers': kafka_bootstrap_servers,
        'value_serializer': lambda v: json.dumps(v).encode('utf-8')
    } 
   
   producer = KafkaProducer(**kafka_producer_config)

   # Conectando ao MongoDB
   client = MongoClient(mongodb_uri)
   db = client[mongodb_database]
   collection = db[mongodb_collection]


   for document in collection.find():
         # Criando estrutura final
         transformed_data = {
            'codigo_municipio': document.get('codigo_municipio'),
            'municipio': document.get('municipio'),
            'ano': document.get('ano'),
            'mes': document.get('mes'),
            'tipo_de_crime': document.get('tipo_de_crime'),
            'qtd_registros': document.get('qtd_registros')
         }

         #enviando para o Kafka
         producer.send(kafka_topic, key=None, value=transformed_data)

   # Fechando conexões
   producer.flush()
   producer.close()         
   client.close()
   remove_refined_duplicates()

def remove_refined_duplicates():
   # configurações mongo
   mongodb_uri='mongodb://root:root@mongodb/'
   mongodb_database='tcc'
   mongodb_collection='tcc_refined'

   # Conectando ao MongoDB
   client = MongoClient(mongodb_uri)
   db = client[mongodb_database]
   collection = db[mongodb_collection]

   try:
      # Criar um DataFrame a partir dos documentos MongoDB
      df = pd.DataFrame(list(collection.find()))

      # Especificar as colunas usadas para identificar duplicatas
      columns_to_check = ["ano", "mes", "codigo_municipio",'tipo_de_crime','qtd_registros']

      # Encontrar e manter apenas a primeira ocorrência de cada duplicata
      df_no_duplicates = df.drop_duplicates(subset=columns_to_check, keep="last")

      # Remover todos os documentos da coleção
      collection.delete_many({})

      # Inserir os documentos não duplicados de volta na coleção
      collection.insert_many(df_no_duplicates.to_dict(orient="records"))

      print("Duplicatas removidas com sucesso.")

   except Exception as e:
      print(f"Ocorreu um erro: {e}")

   finally:
      # Fechar a conexão com o MongoDB
      client.close()   

def transfer_data_mongodb_to_mysql():
    # Configurações MongoDB
    mongodb_uri = 'mongodb://root:root@mongodb/'
    mongodb_database = 'tcc'
    mongodb_collection = 'tcc_refined'

    # Configurações MySQL
    mysql_host = 'mysql'
    mysql_user = 'root'
    mysql_password = 'root'
    mysql_database = 'tcc_final_data'

    # Conectar ao MongoDB
    mongo_client = MongoClient(mongodb_uri)
    mongo_db = mongo_client[mongodb_database]
    mongo_collection = mongo_db[mongodb_collection]

    # Conectar ao MySQL
    mysql_connection = mysql.connector.connect(
        host=mysql_host,
        user=mysql_user,
        password=mysql_password,
        database=mysql_database
    )
    mysql_cursor = mysql_connection.cursor()

    try:
        # Obter dados do MongoDB
        mongo_data = list(mongo_collection.find())

        # Inserir dados no MySQL
        for document in mongo_data:
            mysql_cursor.execute(
                "INSERT INTO `crimes` (`codigo_municipio`, `municipio`, `ano`, `mes`, `tipo_de_crime`, `qtd_registros`) "
                "VALUES (%s, %s, %s, %s, %s, %s) "
                "ON DUPLICATE KEY UPDATE `qtd_registros` = if(values(`qtd_registros`) <> `qtd_registros`, values(`qtd_registros`), `qtd_registros`);",
                (
                    document.get('codigo_municipio'),
                    document.get('municipio'),
                    document.get('ano'),
                    document.get('mes'),
                    document.get('tipo_de_crime'),
                    document.get('qtd_registros')

                )
            )

        # Commit das alterações no MySQL
        mysql_connection.commit()

    finally:
        # Fechar conexões
        mongo_client.close()
        mysql_cursor.close()
        mysql_connection.close()   


