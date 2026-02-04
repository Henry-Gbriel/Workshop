from src.services.pokeapi import PokeAPIService # Acessa os Dados da PokeAPI
from src.loaders.databricks import DatabricksLoader # Local de destino dos dados
from src.parsers.parser import PokemonParser # Organizador dos dados

LIMIT = 15


class Runner:
    def __init__(self): # Definições iniciais 
        self.service = PokeAPIService()
        self.parser = PokemonParser()
        self.destination = DatabricksLoader()
        self.endpoint = "pokemon"
        self.offset = 0

    def run(self):
        print("Starting PokeAPI Ingestion")

        self.destination.open_connection() # Conexão com o Databricks

        records = []
        '''
        Por que ele utiliza um loop infinito (WHILE) aqui?

            Quando trabalhamos com uma lista finita com inicio e fim mas 
            não sabemos quando termina só quando começa utilizamos o while
            para percorrer e grantir que todos os dados serão coletados.
        '''
        while True:
            #Conexão com a PokeAPI
            response = self.service._request(
                method="GET",
                endpoint=self.endpoint,
                params={"offset": self.offset, "limit": LIMIT},
            )

            if response and response.status_code == 200:
                records += response.json().get("results", [])
                next_page = response.json().get("next", None)

            else:
                print("Failed to fetch data from PokeAPI")

            self.offset += LIMIT # limite de registros por requisição --- 15 unidades 
            if next_page is None or self.offset >= 300: #até 300 registros
                break

        parsed_records = self.parser.parse(records) #Organiza os dados

        self.destination.load_records(pandas_df=parsed_records, table="pokemon") #Carrega os dados no Databricks
        self.destination.close_connection() #Fim conexão 

        print("Finished PokeAPI Ingestion")