from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import Integer

from .base_entity import BaseEntity

class BneEntity(BaseEntity):
    __tablename__ = 'bne'
    id = Column(Integer, ForeignKey('base_entity.internal_id'), primary_key=True, autoincrement=True)
    
    # TODO define missing non-standard fields like:
    # alias = Column(String)