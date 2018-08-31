from sqlalchemy import Column
from sqlalchemy import Index
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import Date
from sqlalchemy.engine import Engine
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class BaseEntity(Base):
    __tablename__ = "base_entity"
    id = Column(Integer, unique=True, primary_key=True, autoincrement=True)

    key = Column(String, unique=True) # external, uri key
    name = Column(String) # person name (<name> <surname>)
    birth_date = Column(Date) # date of birth (express date precision)
    death_date = Column(Date) # date of death (express date precision)
    link = Column(String) # link of a related resource
    link_domain = Column(String) # domain of the link

    Index("name_index", name)

    def __repr__(self) -> str:
        return "<BaseEntity(key='{0}', name='{1}')>".format(self.key, self.name)

    def drop(self, engine: Engine) -> None:
        self.__table__.drop(engine)