

Este código define um pipeline de dados no Airflow, estruturado como uma DAG (Directed Acyclic Graph), que processa um arquivo JSON contendo informações de hotéis, 
aplica transformações nos dados, grava resultados intermediários em arquivos CSV e os carrega em um Data Warehouse (DW):


=> O que o pipeline faz:

1. Carrega Dados da Fonte:

	- Lê o arquivo JSON dados_hoteis.json contendo os dados brutos de hotéis.

2. Aplica Transformações:

	- Extrai atributos como nome, tipo, avaliações e prêmios.
	- Divide os dados em dimensões específicas: preço e ranking, prêmios e classificações, número de avaliações, etc.
	- Grava essas transformações em arquivos CSV intermediários no diretório stage.

3. Carrega Dados no Data Warehouse (DW):

	- Insere os dados transformados em tabelas dimensionais no DW.
	- Calcula métricas agregadas (como máximo, mínimo e contagem) e insere esses valores na tabela fato no DW.

4. Ordem de Execução:

	- Define a sequência de tarefas no pipeline usando o Airflow, garantindo que cada tarefa dependa da anterior.



=> Componentes Principais:

1. Funções Python

	- carrega_arquivo():

		- Carrega o arquivo JSON da fonte de dados.

	- Funções de Extração (func_extrai_atributos, func_prices_range_position, func_hotel_award_rating,   
	                       func_reviews_class_rating):

		- Extraem subconjuntos de dados específicos e os gravam em arquivos CSV:

			- Atributos principais do hotel.
			- Faixas de preço e posições no ranking.
			- Nomes, prêmios e avaliações.

	- Função para Inserir Dimensões (func_insert_dimensions):

		- Lê os arquivos CSV gerados nas etapas anteriores e os insere em tabelas dimensionais no DW.
		- Limpa as tabelas antes de inserir os novos dados.

	- Função para Inserir na Tabela Fato (func_insert_fact):

		- Agrega dados das tabelas dimensionais para inserir na tabela fato.
		- Calcula métricas como o número de hotéis, máximos e mínimos de classe e número de avaliações.



2. Definição da DAG no Airflow

A DAG (Directed Acyclic Graph) organiza as tarefas e suas dependências:

	- Cada tarefa é representada por um operador Python (PythonOperator).

	- Dependências:

		- As tarefas são conectadas para formar um fluxo lógico.


Tarefas da DAG:

	1. task1_carrega_dados:

		- Lê o arquivo JSON.

	2. task2_extrai_dados:

		- Extrai atributos gerais dos hotéis.

	3. task3_prices_range_position:

		- Extrai o intervalo de preços e a posição no ranking.

	4. task4_hotel_award_rating:

		- Extrai prêmios e classificações dos hotéis.

	5. task5_reviews_class_rating:

		- Extrai o número de avaliações, classes de hotéis e classificações.

	6. task6_insert_dimensions:

		- Insere os dados nas tabelas dimensionais no DW.

	7. task7_insert_fact:

		- Insere os dados na tabela fato com agregações.



3. Dependências entre Tarefas

O pipeline segue a seguinte ordem:

carrega_dados -> transforma_dados -> prices_range_position -> hotel_award_rating -> reviews_class_rating -> insert_dimensions -> insert_fact


Fluxo do Pipeline

	1. Entrada:

		- Arquivo JSON com dados dos hotéis, localizado no diretório entrada.

	2. Transformação:

		- Criação de arquivos CSV intermediários:

			resultado1.csv: Dados gerais.
			resultado2.csv: Faixas de preço e ranking.
			resultado3.csv: Prêmios e classificações.
			resultado4.csv: Avaliações e classes.

	3. Carga:

		- Dados transformados são carregados em tabelas no DW:

			- Tabelas dimensionais (tb_prices_rang, tb_hotels, tb_hotel_reviews).
			- Tabela fato (tb_fact).

	4. Resultados no DW:

		- Os dados finais estão disponíveis para consultas analíticas e geração de relatórios.
