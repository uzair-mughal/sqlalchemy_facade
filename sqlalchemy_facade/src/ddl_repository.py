from typing import List
from sqlalchemy.engine import Engine
from sqlalchemy.schema import Sequence
from sqlalchemy_facade.src.declarative_base import Entity
from sqlalchemy.schema import CreateSchema, CreateSequence


class DDLRepository:
    def __init__(self, engine: Engine):
        self.__engine = engine

    def create_schemas(self, schemas: List[str]):
        """
        Creates schema.
        """

        for schema in schemas:
            with self.__engine.begin() as connection:
                try:
                    connection.execute(CreateSchema(name=schema))
                except Exception:
                    pass

    def create_tables(self):
        """
        Creates tables. Tables of all models that are inherited from Entity will be created.
        """

        with self.__engine.begin() as connection:
            Entity.metadata.create_all(connection)

    def drop_tables(self):
        """
        Drop tables. Tables of all models that are inherited from Entity will be dropped.
        """

        with self.__engine.begin() as connection:
            Entity.metadata.drop_all(connection)

    def create_sequences(self, sequences: List[str]):
        for sequence in sequences:
            with self.__engine.begin() as connection:
                schema = sequence.split(".")[0]
                name = sequence.split(".")[1]

                connection.execute(CreateSequence(Sequence(name=name, start=1, increment=1, schema=schema)))

    def seed_data(self, table, data: List[dict]):
        with self.__engine.begin() as connection:
            connection.execute(table.insert(), data)
