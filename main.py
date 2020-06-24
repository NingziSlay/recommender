# coding: utf-8
"""
推荐系统主入口
"""
from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from starlette import status
from starlette.background import BackgroundTasks

from recommender import FileReader
from schema import TrackModel
from utils import SessionMaker, group_query_result

app = FastAPI()

query_string = """
select t.id as 'track_id',
       artist,
       `desc`,
       tag.name
from (select id, artist, `desc` from track {}) t
         left outer join track_tag_rel ttr on t.id = ttr.track_id
         left outer join tag on ttr.tag_id = tag.id
"""


@app.get("/update-track")
async def update_csv(
        background_tasks: BackgroundTasks,
        limit: int = 0,
        session: Session = Depends(SessionMaker)
):
    if limit:
        query = query_string.format(f"limit {limit}")
    else:
        query = query_string.format("")

    tracks = session.execute(query)
    track_group = group_query_result(tracks, group_field="track_id")
    tracks = [TrackModel(**tracks[0], tags=tracks) for tracks in track_group]
    f = FileReader()
    background_tasks.add_task(f.write_to_csv, tracks)
    return


@app.get("/recommend")
async def recommend(track_id: int, count: int = 6):
    pass


if __name__ == '__main__':
    import uvicorn

    uvicorn.run(app)
