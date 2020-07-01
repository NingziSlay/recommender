# coding: utf-8

from pydantic import BaseSettings, AnyUrl, BaseModel
from pydantic import RedisDsn as _RedisDsn
import os

__all__ = ["Configs"]


class RedisDsn(_RedisDsn):
    user_required = False


_BASE_DIR = os.path.dirname(os.path.abspath(__file__))


class _SparkConfig(BaseModel):
    AppName: str
    Master: str


class _Files(BaseModel):
    track_path: str = os.path.join(_BASE_DIR, "data/track.csv")
    similarity_model: str = os.path.join(_BASE_DIR, "data/similarity_model")
    indices_path: str = os.path.join(_BASE_DIR, "data/indices.csv")


class BaseConfig(BaseSettings):
    SparkConfig: _SparkConfig
    DB_URL: AnyUrl
    # REDIS_URL: RedisDsn
    BASE_DIR: str = _BASE_DIR
    FILES: _Files = _Files()

    class Config:
        case_sensitive = True


class ProdConfigs(BaseConfig):
    pass


class DevConfigs(BaseConfig):
    SparkConfig: _SparkConfig = _SparkConfig(AppName="demo", Master="spark://192.168.0.174:7077")
    DB_URL: AnyUrl = "mysql+pymysql://root:root@192.168.0.11:3306/coolvox_dev?charset=utf8mb4&use_unicode=1"
    # REDIS_URL: RedisDsn = "redis://192.168.0.11:6379/1"


def GetSettings(env: str) -> BaseConfig:
    if env == "dev":
        return DevConfigs()
    elif env == "prod":
        raise NotImplementedError


Configs: BaseConfig = GetSettings(os.environ.get("ENVIRONMENT", "dev"))
