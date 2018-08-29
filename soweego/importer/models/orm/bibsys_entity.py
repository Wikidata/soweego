from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import Boolean
from sqlalchemy import Table

from .orm_entity import OrmEntity

class BibsysEntity(OrmEntity):
    __tablename__ = 'bibsys'
    uri = Column(Integer, unique=True)

    # name
    name = Column(String)
    catalogue_name = Column(String)
    label = Column(String)
    alt_label = Column(String)
    
    # dates
    since = Column(Integer)
    until = Column(Integer)
    modified = Column(String)

    note = Column(String)
    suffix_name = Column(String) 

    isPerson = Column(Boolean)

    def __repr__(self) -> str:
        return "<BibsysEntity(id='{0}', name='{1}')>".format(self.id, self.name)
