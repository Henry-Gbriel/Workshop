from src.services.pokeapi import PokeAPIService
from src.loaders.databricks import DatabricksLoader
from src.parsers.parser import PokemonParser

LIMIT = 15


class Runner:
    def __init__(self):
        self.service = PokeAPIService()
        self.parser = PokemonParser()
        self.destination = DatabricksLoader()
        self.endpoint = "pokemon"
        self.offset = 0

    def run(self):
        print("Starting PokeAPI Ingestion")

        self.destination.open_connection()

        records = []
        while True:
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

            self.offset += LIMIT
            if next_page is None or self.offset >= 300:
                break

        parsed_records = self.parser.parse(records)

        self.destination.load_records(pandas_df=parsed_records, table="pokemon")
        self.destination.close_connection()

        print("Finished PokeAPI Ingestion")