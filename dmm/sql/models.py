from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from datetime import datetime

from dmm.sql.session import get_engine

BASE = declarative_base()

class ModelBase(object):
    def __setitem__(self, key, value):
        setattr(self, key, value)

    def __getitem__(self, key):
        return getattr(self, key)
    
    @declared_attr
    def created_at(cls):
        return Column("created_at", DateTime, default=datetime.utcnow)

    @declared_attr
    def updated_at(cls):
        return Column("updated_at", DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def save(self, session=None):
        """Save this object"""
        session.add(self)
    
    def delete(self, session=None):
        """Delete this object"""
        session.delete(self)

    def update(self, values, session=None):
        """dict.update() behaviour."""
        for k, v in values.items():
            self[k] = v

# DMM Data Structures
class Request(BASE, ModelBase):
    __tablename__ = "requests"
    request_id = Column(String(255), primary_key=True)
    rule_id = Column(String(255))
    src_site = Column(String(255))
    dst_site = Column(String(255))
    transfer_ids = relationship('FTSTransfer', back_populates='request')  
    priority = Column(Integer())
    n_bytes_total = Column(Integer())
    n_transfers_total = Column(Integer())
    src_ipv6 = Column(String(255))
    dst_ipv6 = Column(String(255))
    bandwidth = Column(Float())
    sense_link_id = Column(String(255))
    transfer_status = Column(String(255))

    def __init__(self, **kwargs):
        super(Request, self).__init__(**kwargs)
    
class FTSTransfer(BASE, ModelBase):
    __tablename__ = "ftstransfers"
    id = Column(Integer(), autoincrement=True, primary_key=True)
    value = Column(String(255))    
    request_id = Column(String(255), ForeignKey('requests.request_id'))  
    request = relationship('Request', back_populates='transfer_ids')

    def __init__(self, **kwargs):
        super(FTSTransfer, self).__init__(**kwargs)

# Create the tables if don't exist when module first imported.
engine=get_engine()
Request.__table__.create(bind=engine, checkfirst=True)
FTSTransfer.__table__.create(bind=engine, checkfirst=True)