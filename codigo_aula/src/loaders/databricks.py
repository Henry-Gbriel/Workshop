#Basicamente aceeso o dado organizado em parser.py e salvo na tabela delta no databricks

import pandas as pd
from databricks.connect import DatabricksSession
from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp

# from databricks.connect.cache import clear_cache

CATALOG = "jornada"
SCHEMA = "databricks"


class DatabricksLoader:
    def __init__(self) -> None:
        self.session = None

    def open_connection(self) -> None:
        self.session = DatabricksSession.builder.serverless().getOrCreate() #Cria a sessão do databricks

    def close_connection(self) -> None:
        if self.session is not None: #Se a sessão existir
            try:
                self.session.stop() #Invalida a sessão
            except:
                pass
            self.session = None # Limpa a sessão

    def load_records(
        self, pandas_df: pd.DataFrame, table: str, pk_columns: list = None
    ) -> None:
        assert self.session is not None #Assegura que a sessão foi criada

        print(f"Loading records into Databricks table: {table}")

        table_path = f"{CATALOG}.{SCHEMA}.{table}" #Caminho completo da tabela

        self.session.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
        self.session.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

        df = self.session.createDataFrame(pandas_df)
        df = df.withColumn("ingestion_date", current_timestamp())

        df.write.format("delta").mode("overwrite").saveAsTable(table_path) #Salva o dataframe na tabela delta