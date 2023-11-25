import logging
from sqlalchemy.future import select
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager
from sqlalchemy.sql.expression import delete
from sqlalchemy.engine import Engine
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy_facade.src.declarative_base import Entity

logger = logging.getLogger(__name__)

class WriteRepository:
    def __init__(self, engine: Engine):
        self.__engine = engine
        self.__session = None

    @contextmanager
    def session(self):
        """
        Context manager, yields the same shared session every time it is requested.
        Once the call is returned back to this function, the finally block is triggered
        which closes the session.
        """

        try:
            if self.__session is None:
                Session = sessionmaker(bind=self.__engine, autoflush=False, autocommit=False)

                self.__session = Session()
                logger.debug("Sqlalchemy new session created.")

            yield self.__session
            self.__session.flush()
        except Exception as e:
            self.close()
            logger.error(e, exc_info=True)
            raise

    def start_transaction(self):
        """
        Starts the transaction on the session object.
        """

        with self.session() as session:
            session.begin()

    def close(self):
        if self.__session is not None:
            self.__session.close()
            logger.debug("Sqlalchemy session closed.")
            self.__session = None

    def commit(self):
        """
        Commits the transaction and closes the session.
        """

        with self.session() as session:
            session.commit()

        self.close()

    def rollback(self):
        """
        Rollbacks transaction and closes session.
        """
        with self.session() as session:
            session.rollback()

    def insert(self, row):
        """
        Inserts a single row in the database.
        """

        try:
            with self.session() as session:
                session.add(row)
        except Exception as e:
            self.close()
            raise

    def upsert(self, entity: Entity, index_elements: list = ["id"]):
        with self.session() as session:
            properties = entity.__dict__.copy()
            properties.pop("_sa_instance_state")

            query = (
                insert(entity.__table__)
                .values(properties)
                .on_conflict_do_update(index_elements=index_elements, set_=properties)
            )
            session.execute(query)
            session.flush()

    def insert_ignore(self, entity: Entity):
        with self.session() as session:
            properties = entity.__dict__.copy()
            properties.pop("_sa_instance_state")

            query = insert(entity.__table__, properties).on_conflict_do_nothing()
            session.execute(query)
            session.flush()

    def bulk_insert(self, rows):
        """
        Inserts multiple rows in the database.
        """

        try:
            with self.session() as session:
                session.add_all(rows)
        except Exception as e:
            self.close()
            raise

    def update(self, model, filters, properties):
        """
        Updates one or more rows that match the given filters.
        """

        try:
            with self.session() as session:
                rows = session.execute(select(model).filter_by(**filters))

                rows = [row for row, in rows.fetchall()]

                for row in rows:
                    for key, value in properties.items():
                        setattr(row, key, value)
        except Exception as e:
            self.close()
            raise

    def delete(self, model, filters):
        """
        Deletes one or more rows that match the given filters.
        """

        try:
            with self.session() as session:
                session.execute(delete(model).filter_by(**filters))
        except Exception as e:
            self.close()
            raise

    def execute(self, statement):
        with self.session() as session:
            session.execute(statement)

    def execute_statement(self, statement):
        with self.session() as session:
            session.execute(statement)
