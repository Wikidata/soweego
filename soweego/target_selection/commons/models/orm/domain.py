from sqlalchemy import Column, String
from .orm_entity import OrmEntity

class Domain(OrmEntity):
    __tablename__ = 'domain'
    name = Column(String)
    base_uri = Column(String)

    def __repr__(self) -> str:
       return "<User(name='{0}', base_uri='{1}')>".format(self.name, self.base_uri)