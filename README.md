# tcc_puc_final
Trabalho de Conclusão do Curso de Engenharia de Dados da Puc Minas.

## Passos
Nessa seção disponibilizo as etapas com os respectivos comandos que precisam ser executados para execução do projeto após a clonagem.

1. ativar o ambiente virtual 
    python -m venv venv
    .\venv\Scripts\activate
2. Instalar os pacotes necessários
    pip install confluent_kafka apache-airflow-providers-apache-kafka kafka-python pymongo mysql-connector-python
3. Iniciar o Docker
4. Criar o arquivo .env com o conteúdo abaixo
    AIRFLOW_UID=50000
    ALLOW_ANONYMOUNS_LOGIN=yes

5. Inicializar os containers seguindo a ordem estabelecida abaixo:
    docker-compose up airflow-init
    docker-compose up -d
6. Entrar no airflow-scheduler e arflow-worker e instalar os pacotes necessários para as DAGs
    docker-compose exec -it airflow-scheduler /bin/bash
    pip install confluent_kafka apache-airflow-providers-apache-kafka kafka-python pymongo mysql-connector-python
    exit
    docker-compose exec -it airflow-worker /bin/bash
    pip install confluent_kafka apache-airflow-providers-apache-kafka kafka-python pymongo mysql-connector-python
    exit
7. Logar no MySQL e Criar as tabelas
    docker-compose exec -it mysql /bin/bash
    mysql -uroot -proot
    use tcc_final_data;
    drop table if exists `crimes`;
    CREATE TABLE `crimes` (
    `codigo_municipio` int(11) NOT NULL,
    `municipio` varchar(255) DEFAULT NULL,
    `ano` varchar(4) NOT NULL,
    `mes` varchar(20) NOT NULL,
    `tipo_de_crime` varchar(255) NOT NULL,
    `qtd_registros` int(11) DEFAULT NULL,
    UNIQUE KEY `codigo_municipio` (`codigo_municipio`,`ano`,`mes`,`tipo_de_crime`),
    KEY `periodo` (`ano`,`mes`)
    ) ENGINE=InnoDB;

    drop table if exists `localidades`;
    CREATE TABLE `localidades` (
    `cod_cid` int(11) DEFAULT NULL,
    `mesorregiao` varchar(255) DEFAULT NULL,
    `uf` varchar(2) DEFAULT NULL,
    `estado` varchar(255) DEFAULT NULL,
    UNIQUE KEY `cod_cid` (`cod_cid`)
    ) ENGINE=InnoDB;
    exit
    exit

## Links de Acesso das Interfaces
    - [Airflow] - localhost:8080
        - Usuário: airflow
        - Senha: airflow
    - [Kafka Control Center] - localhost:9021
    - [Mongo Express] - localhost:8085
        - Usuário: admin
        - Senha: pass

## Configuração Adicional
    Acessar a interface do Kafka Control Center
    No menu lateral clicar em Cluster 1
    Na página seguinte clicar em connect -> connect-default -> Add Connector -> Upload connector config file
    selecionar o conector no diretório connectors do projeto em se seguida clicar em continue -> Launch
        * Esse processo deve ser repetido para os 3 conectores presentes no diretório
    Com a configuração acima feita os tópicos Kafka estarão criados.