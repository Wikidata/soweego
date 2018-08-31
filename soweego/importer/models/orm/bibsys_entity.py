from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import String
from sqlalchemy import Integer

from .base_entity import BaseEntity

class BibsysEntity(BaseEntity):
    __tablename__ = "bibsys"

    id = Column(Integer, ForeignKey('base_entity.id'), primary_key=True, autoincrement=True)
    catalogue_name = Column(String)
    label = Column(String)
    alt_label = Column(String)
    modified = Column(String)
    note = Column(String)
    entity_type = Column(String)