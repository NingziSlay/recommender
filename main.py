# coding: utf-8
"""
推荐系统主入口
"""
import linecache
from fastapi import FastAPI, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from starlette import status
from starlette.background import BackgroundTasks
from configs import Configs
from recommender import UpdateSimilarityModel
from utils import SessionMaker
from Globals import global_vars

app = FastAPI()


@app.get("/update-track")
async def update_csv(
        background_tasks: BackgroundTasks,
        limit: int = 0,
        session: Session = Depends(SessionMaker)
):
    if global_vars.CALCULATE_STATUS:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="calculating..."
        )
    global_vars.CALCULATE_STATUS = True

    background_tasks.add_task(UpdateSimilarityModel, session, limit)
    return


@app.get("/recommend")
async def recommend(track_id: int, count: int = Query(6, le=10, gt=0)):
    try:
        track_index = global_vars.REVERSED_INDICES[track_id]
    except KeyError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Source Not Found"
        )

    target = global_vars.DATA.loc[track_index]
    target = f"{target.track_id}: {target.description}"

    sims = linecache.getline(Configs.FILES.similarity_model, track_index + 1)
    if not sims:
        return []
    sims = eval(sims.strip())[:count]
    result = global_vars.DATA.loc[[s[0] for s in sims]]
    matches = list(map(lambda x, y: f"{x}: {y}", result.track_id, result.description))
    return dict(target=target, matches=matches)
    # tracks = global_vars.INDICES.iloc[[s[0] for s in sims]]
    # return tracks.track_id.to_list()


if __name__ == '__main__':
    import uvicorn

    # 初始化歌曲索引
    global_vars.init_indices()

    uvicorn.run(
        app,
        host="0.0.0.0"
    )
