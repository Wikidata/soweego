from sqlalchemy import Column
from sqlalchemy import String
from sqlalchemy import Integer
from sqlalchemy import ForeignKey

from .base_entity import BaseEntity

class MusicbrainzEntity(BaseEntity):
    __tablename__ = 'musicbrainz'
    id = Column(Integer, ForeignKey('base_entity.internal_id'), primary_key=True, autoincrement=True)
    
    # TODO define missing non-standard fields like:
    # alias = Column(String)