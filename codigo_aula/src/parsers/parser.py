import pandas as pd

#Transforma a lista de dicionários em um DataFrame do pandas

class PokemonParser:
    def __init__(self):
        pass # não faz nada na inicialização

    def parse(self, records: list) -> pd.DataFrame:
        df = pd.DataFrame(records) # converte a lista de dicionários em DataFrame

        print("Parsed records into DataFrame, total records:", df.shape[0])

        return df