# coding: utf-8
"""
推荐系统主入口
"""
import linecache

import pandas
from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from starlette import status
from starlette.background import BackgroundTasks
from configs import Configs
from recommender import FileReader
from schema import TrackModel
from utils import SessionMaker, group_query_result
from glo_vars import CALCULATE_STATUS, INDICES, REVERSED_INDICES

app = FastAPI()


@app.get("/update-track")
async def update_csv(
        background_tasks: BackgroundTasks,
        limit: int = 0,
        session: Session = Depends(SessionMaker)
):
    query_string = """
    select t.id as 'track_id',
           artist,
           `desc`,
           tag.name
    from (select id, artist, `desc` from track {}) t
             left outer join track_tag_rel ttr on t.id = ttr.track_id
             left outer join tag on ttr.tag_id = tag.id
    """

    global CALCULATE_STATUS

    if CALCULATE_STATUS:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="calculating..."
        )

    CALCULATE_STATUS = True

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
    try:
        track_index = REVERSED_INDICES[track_id]
    except KeyError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Source Not Found"
        )
    sims = linecache.getline(Configs.FILES.similarity_model, track_index + 1)
    sims = eval(sims)[:count]
    tracks = INDICES.iloc[[s[0] for s in sims]]
    return tracks.track_id.to_list()


if __name__ == '__main__':
    """
    项目启动时，从文件中读取索引和反向索引。
    当数据更新时，更新成功后，要把索引对象替换为更新后的
    """
    INDICES = pandas.read_csv(Configs.FILES.indices_path, names=["track_id", "index"])
    REVERSED_INDICES = pandas.Series(INDICES["index"].values, index=INDICES["track_id"])

    import uvicorn

    uvicorn.run(app)
