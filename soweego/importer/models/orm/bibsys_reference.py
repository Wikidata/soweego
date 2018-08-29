from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String 

from .orm_entity import OrmEntity

class BibsysReference(OrmEntity):
    __tablename__ = 'reference'
    url = Column(String)
    sameAs = Column(Integer)
    domain = Column(Integer)
    
    def __repr__(self) -> str:
       return "<BibsysReference(url='{0}', sameAs='{1}')>".format(self.url, self.sameAs)