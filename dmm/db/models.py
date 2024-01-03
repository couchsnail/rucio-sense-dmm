from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from datetime import datetime

import json

from dmm.db.session import get_engine
from dmm.utils.sense import get_site_info

from dmm.utils.config import config_get

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
    rule_id = Column(String(255), primary_key=True)
    src_site = Column(String(255))
    dst_site = Column(String(255))
    priority = Column(Integer())
    n_bytes_total = Column(Integer())
    src_ipv6_block = Column(String(255))
    dst_ipv6_block = Column(String(255))
    src_url = Column(String(255))
    dst_url = Column(String(255))
    bandwidth = Column(Float())
    sense_link_id = Column(String(255))
    transfer_status = Column(String(255))

    def __init__(self, **kwargs):
        super(Request, self).__init__(**kwargs)
        self.n_transfers_submitted = 0
        self.n_bytes_transferred = 0
        self.n_transfers_finished = 0

class Site(BASE, ModelBase):
    __tablename__ = "sites"
    id = Column(Integer(), autoincrement=True, primary_key=True)
    name = Column(String(255))
    sense_uri = Column(String(255))
    port_capacity = Column(Integer())
    query_url = Column(String(255))

    def __init__(self, **kwargs):
        super(Site, self).__init__(**kwargs)
        vlan_tag = config_get("sense", "vlan_tag", default="Any")
        site_info = get_site_info(self.name)
        site_info = json.loads(site_info)
        self.sense_uri = site_info["domain_uri"]
        for port in site_info["peer_points"]:
            if vlan_tag.split("-")[0] in port["port_vlan_pool"].split(","):
                self.port_capacity = port["port_capacity"]
        self.query_url = site_info["domain_url"]

# Create the tables if don't exist when module first imported.
engine=get_engine()
Request.__table__.create(bind=engine, checkfirst=True)
Site.__table__.create(bind=engine, checkfirst=True)