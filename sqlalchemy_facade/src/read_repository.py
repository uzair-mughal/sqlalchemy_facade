import logging
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager
from sqlalchemy import select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

class ReadRepository:
    def __init__(self, engine: Engine):
        self.__engine = engine
        self.__session = None

    @contextmanager
    def session(self):
        """
        Context manager, yields a new session every time it is requested.
        Once the call is returned back to this function, the finally block is triggered
        which closes the session.
        """

        try:
            Session = sessionmaker(bind=self.__engine, autoflush=False, autocommit=False)

            self.__session = Session()

            logger.debug("Sqlalchemy new session created.")
            yield self.__session
        except Exception as e:
            logger.error(e, exc_info=True)
            self.close()
            raise
        finally:
            self.close()

    def close(self):
        if self.__session is not None:
            self.__session.close()
            logger.debug("Sqlalchemy session closed.")
            self.__session = None

    def select(self, model, filters: dict):
        """
        Selects a single row from the table specified by the model while
        applying the given filters.
        """

        with self.session() as session:
            rows = session.execute(select(model).filter_by(**filters))

            try:
                return rows.fetchone()[0]
            except Exception:
                return None

    def select_all(self, model, filters: dict):
        """
        Selects multiple rows from the table specified by the model while
        applying the given filters.
        """

        with self.session() as session:
            rows = session.execute(select(model).filter_by(**filters))

            return [row for row, in rows.fetchall()]

    def execute(self, statement):
        with self.session() as session:
            rows = session.execute(statement)
            return rows.fetchall()

    def execute_in_batch(self, statement, batch_size: int = 1000):
        with self.session() as session:
            result_proxy = session.execute(statement)
            while True:
                batch = result_proxy.fetchmany(batch_size)
                if not batch:
                    break
                yield batch
