# coding: utf-8

"""
公共方法库
"""
from collections import defaultdict
from typing import List

import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

from configs import Configs


class Singleton(type):
    """单例元类
    usage:
        >>> class A(metaclass=Singleton):
        >>>     pass
        >>> a = A()
        >>> b = A()
        >>> assert a == b
    """
    def __init__(cls, *args, **kwargs):
        cls.__instance = None
        super().__init__(*args, **kwargs)

    def __call__(cls, *args, **kwargs):
        if cls.__instance is None:
            cls.__instance = super().__call__(*args, **kwargs)
        return cls.__instance


class SessionMaker(object):
    _session = None

    def __new__(cls) -> Session:
        if not cls._session:
            engine = create_engine(
                Configs.DB_URL,
                echo_pool=True,
                pool_recycle=3600,
                pool_size=100,
                max_overflow=200,
                pool_timeout=300
            )
            cls._session: Session = sessionmaker(bind=engine)()
        return cls._session


def group_query_result(
        query_result: List[sqlalchemy.engine.result.RowProxy],
        *,
        group_field: str
) -> List[List[sqlalchemy.engine.result.RowProxy]]:
    """ 把 sqlalchemy core 的查询返回值按照传入的字段名来分组
    :param query_result: sql查询结果
    :param group_field: 用来分组的字段
    """
    group = defaultdict(list)
    for row in query_result:
        if group_field in row.keys():
            group[row[group_field]].append(row)
    return list(group.values())
