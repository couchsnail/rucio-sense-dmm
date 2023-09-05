from sqlalchemy import Column, Integer, String, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base, declared_attr

from datetime import datetime

BASE = declarative_base()

class ModelBase(object):
    @declared_attr
    def created_at(cls):  # pylint: disable=no-self-argument
        return Column("created_at", DateTime, default=datetime.utcnow)

    @declared_attr
    def updated_at(cls):  # pylint: disable=no-self-argument
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

class Request(BASE, ModelBase):
    __tablename__ = "Requests"
    request_id = Column(String(255), primary_key=True)
    rule_id = Column(String(255))
    src_site = Column(String(255))
    dst_site = Column(String(255))
    transfer_ids = Column(String(255)) # CSV
    priority = Column(Integer())
    n_bytes_total = Column(Integer())
    n_transfers_total = Column(Integer())
    src_ipv6 = Column(String(255))
    dst_ipv6 = Column(String(255))
    bandwidth = Column(Float())
    sense_link_id = Column(String(255))
    transfer_status = Column(String(255))