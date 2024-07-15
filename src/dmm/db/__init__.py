from dmm.db.models import *
from dmm.db.session import get_engine

engine=get_engine()
BASE.metadata.create_all(bind=engine, checkfirst=True)