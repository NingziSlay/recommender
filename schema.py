# coding: utf-8
import typing
from pydantic import BaseModel


class TagModel(BaseModel):
    name: typing.Optional[str] = ""


class TrackModel(BaseModel):
    track_id: int
    artist: str
    desc: str
    tags: typing.List[TagModel]
