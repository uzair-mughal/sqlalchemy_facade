import logging
from sqlalchemy.future import select
from sqlalchemy.engine import Engine
from contextlib import contextmanager
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql.expression import delete
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy_facade.src.declarative_base import Entity

# Set up logging for the module.
logger = logging.getLogger(__name__)


class WriteRepository:
    def __init__(self, engine: Engine):
        """
        Initialize the repository with a SQLAlchemy engine.

        Args:
            engine (Engine): A SQLAlchemy Engine object used for database connections.
        """
        self.__engine = engine
        self.__session = None

    @contextmanager
    def session(self):
        """
        A context manager that yields a SQLAlchemy session. It ensures that the same session is
        reused within the context and properly closed after usage.

        Yields:
            Session: A SQLAlchemy session object.
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
        Starts a database transaction on the current session.
        """
        if self.__session is not None:
            self.__session.begin()

    def close(self):
        """
        Closes the current session if it exists.
        """
        if self.__session is not None:
            self.__session.close()
            logger.debug("Sqlalchemy session closed.")
            self.__session = None

    def commit(self):
        """
        Commits the current transaction and closes the session.
        """
        if self.__session is not None:
            self.__session.commit()
            self.close()

    def rollback(self):
        """
        Rollbacks the current transaction and closes the session.
        """
        if self.__session is not None:
            self.__session.rollback()
            self.close()

    def insert(self, row):
        """
        Inserts a single row into the database.

        Args:
            row: The row to be inserted.
        """
        try:
            with self.session() as session:
                session.add(row)
        except Exception as e:
            self.close()
            raise

    def upsert(self, entity: Entity, index_elements: list = ["id"]):
        """
        Performs an 'upsert' operation, which inserts a new row or updates the existing row
        if there's a conflict with the given index elements.

        Args:
            entity (Entity): The entity to be upserted.
            index_elements (list): A list of column names to be used as the conflict target.
        """
        try:
            with self.session() as session:
                properties = entity.__dict__.copy()
                properties.pop("_sa_instance_state")

                query = (
                    insert(entity.__table__)
                    .values(properties)
                    .on_conflict_do_update(index_elements=index_elements, set_=properties)
                )
                session.execute(query)
        except Exception as e:
            self.close()
            raise

    def insert_ignore(self, entity: Entity):
        """
        Performs an insert operation that ignores the insert if a conflict occurs.

        Args:
            entity (Entity): The entity to be inserted.
        """
        try:
            with self.session() as session:
                properties = entity.__dict__.copy()
                properties.pop("_sa_instance_state")

                query = insert(entity.__table__, properties).on_conflict_do_nothing()
                session.execute(query)
        except Exception as e:
            self.close()
            raise

    def insert_ignore_get_id(self, entity: Entity):
        """
        Performs an insert operation that ignores the insert if a conflict occurs and returns
        the primary key of the inserted or existing row.

        Args:
            entity (Entity): The entity to be inserted.

        Returns:
            The primary key of the inserted or existing row.
        """
        try:
            with self.session() as session:
                properties = entity.__dict__.copy()
                properties.pop("_sa_instance_state")

                query = insert(entity.__table__, properties).on_conflict_do_nothing()
                result = session.execute(query)

                if result.inserted_primary_key:
                    return result.inserted_primary_key[0]
                else:
                    non_timestamp_properties = {
                        k: v for k, v in properties.items() if type(v) in [str, int, float, bool]
                    }
                    result = session.execute(select(entity.__table__).filter_by(**non_timestamp_properties))
                    return result.fetchone()[0]
        except Exception as e:
            self.close()
            raise

    def bulk_insert(self, rows):
        """
        Inserts multiple rows into the database.

        Args:
            rows (list): A list of rows to be inserted.
        """
        try:
            with self.session() as session:
                session.add_all(rows)
        except Exception as e:
            self.close()
            raise

    def bulk_upsert(self, rows: list, model: Entity, index_elements: list = ["id"], update_columns: list = None):
        """
        Performs a bulk 'upsert' operation for multiple rows.

        Args:
            rows (list): A list of rows to be upserted.
            model (Entity): The SQLAlchemy model associated with the rows.
            index_elements (list): List of column names to be used as the conflict target.
            update_columns (list): List of column names to be updated in case of a conflict.
        """
        try:
            with self.session() as session:
                if rows:
                    update_dict = {
                        col: getattr(insert(model).excluded, col)
                        for col in (update_columns if update_columns else rows[0].keys())
                    }

                    query = (
                        insert(model)
                        .values(rows)
                        .on_conflict_do_update(index_elements=index_elements, set_=update_dict)
                    )

                    session.execute(query)
        except Exception as e:
            self.close()
            raise

    def update(self, model, filters, properties):
        """
        Updates one or more rows that match the given filters.

        Args:
            model: The SQLAlchemy model to be updated.
            filters: Conditions used to filter the rows to be updated.
            properties: A dictionary of properties to update.
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

        Args:
            model: The SQLAlchemy model from which rows will be deleted.
            filters: Conditions used to filter the rows to be deleted.
        """
        try:
            with self.session() as session:
                session.execute(delete(model).filter_by(**filters))
        except Exception as e:
            self.close()
            raise

    def execute(self, statement):
        """
        Executes a raw SQL statement.

        Args:
            statement: The SQL statement to execute.
        """
        try:
            with self.session() as session:
                session.execute(statement)
        except Exception as e:
            self.close()
            raise