from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import String
from sqlalchemy import Integer, Date, Index
from sqlalchemy.engine import Engine

#from .base_entity import BaseEntity
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class BibsysEntity(Base):
    internal_id = Column(Integer, unique=True,
                         primary_key=True, autoincrement=True)

    # Catalog identifier, indexed
    catalog_id = Column(String(32), nullable=False, index=True)
    # Full name (<name> <surname>)
    name = Column(String(255), nullable=False)
    # Date of birth
    born = Column(Date)
    # Date of birth precision
    born_precision = Column(Integer)
    # Date of death
    died = Column(Date)
    # Date of death precision
    died_precision = Column(Integer)
    # Full-text index over the 'name' column
    Index('name_index', name, mysql_prefix='FULLTEXT')

    __tablename__ = "bibsys"
    #id = Column(Integer, ForeignKey('base_entity.internal_id'), primary_key=True, autoincrement=True)
    catalogue_name = Column(String(255))
    label = Column(String(255))
    alt_label = Column(String(255))
    modified = Column(String(255))
    note = Column(String(255))
    entity_type = Column(String(255))

    def create(self, engine: Engine) -> None:
        Base.metadata.create_all(engine)