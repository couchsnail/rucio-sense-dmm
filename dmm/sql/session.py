from os import environ as env
from functools import wraps

from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy import create_engine
from threading import Lock

_MAKER, _ENGINE, _LOCK = None, None, Lock()

def get_engine():
    global _ENGINE
    if not _ENGINE:
        create_engine("sqlite:///dmm.db")
    assert _ENGINE
    return _ENGINE

def get_maker():
    global _MAKER, _ENGINE
    assert _ENGINE
    if not _MAKER:
        _MAKER = sessionmaker(bind=_ENGINE)
    return _MAKER

def get_session():
    global _MAKER
    if not _MAKER:
        _LOCK.acquire()
        try:
            get_engine()
            get_maker()
        finally:
            _LOCK.release()
    assert _MAKER
    session = scoped_session(_MAKER)
    return session

def databased(function):
    '''
    decorator that set the session variable to use inside a function.
    With that decorator it's possible to use the session variable like if a global variable session is declared.

    session is a sqlalchemy session, and you can get one calling get_session().
    '''
    @wraps(function)
    def new_funct(*args, **kwargs):
        if not kwargs.get('session'):
            session = get_session()
            try:
                kwargs['session'] = session
                result = function(*args, **kwargs)
                session.commit()
            except:
                session.rollback()
                raise
            finally:
                session.remove()
        else:
            result = function(*args, **kwargs)
        return result
    new_funct.__doc__ = function.__doc__
    return new_funct