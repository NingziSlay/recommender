# coding: utf-8
"""
coolvox 的物品相似度推荐
    - 从 coolvox 数据库拉取歌曲信息
    - 歌曲信息的文本特征提取
    - 调用 spark 集群计算文本相似度
    - 数据持久化 TODO 如何持久化
    - 给 coolvox 提供歌曲推荐的接口
    - 新入库的歌曲添加到推荐模型中 TODO 是否可以增量更新

"""
import csv
import linecache
import logging
import os
import re
import subprocess
import time

import jieba
import pandas as pd
import typing

from filelock import FileLock
from scipy.sparse import csr_matrix
from sklearn.feature_extraction.text import CountVectorizer, TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from sqlalchemy.orm import Session

from configs import Configs
from pyspark import SparkConf, SparkContext, RDD

from Globals import global_vars
from schema import TrackModel
from utils import Singleton, group_query_result


def get_features(items: pd.DataFrame):
    """计算文本特征的接口
    """
    vector = CountVectorizer()
    vocabulary = vector.fit(items.clean.values.astype("U")).vocabulary_
    tfidf_vector = TfidfVectorizer(vocabulary=vocabulary)
    return tfidf_vector.fit_transform(items.clean.values.astype("U"))


def broadcast_matrix(mat: csr_matrix) -> csr_matrix:
    """原矩阵广播到多个 workers，等待分片后的每个子矩阵做相似度计算
    """
    casted = sc.broadcast((mat.data, mat.indices, mat.indptr))
    (data, idc, indptr) = casted.value
    return csr_matrix((data, idc, indptr), shape=mat.shape)


def parallelize_matrix(scipy_mat: csr_matrix, rows_per_chunk=100, num_slices=50) -> RDD:
    """将特征矩阵分片，分割成多个 slices，分发给多个 worker
    """
    [rows, cols] = scipy_mat.shape
    i = 0
    sub_matrices = list()
    while i < rows:
        current_chunk_size = min(rows_per_chunk, rows - i)
        sub_mat: csr_matrix = scipy_mat[i: (i + current_chunk_size)]
        sub_matrices.append(
            (i, (sub_mat.data, sub_mat.indices, sub_mat.indptr), (current_chunk_size, cols))
        )
        i += current_chunk_size
    return sc.parallelize(sub_matrices, numSlices=num_slices)


def find_matches_in_sub_matrix(sources: csr_matrix, targets: csr_matrix, input_start_index: int):
    """分割后的子矩阵与原矩阵做余弦相似度计算，最后在汇总在一起
    """
    co_similarities = cosine_similarity(sources, targets)
    for i, similarity in enumerate(co_similarities):
        source_index = i + input_start_index
        # 自身不出现在推荐列表中，直接删除的话会弄乱其他元素的 index，
        # 这里把自身的值改为 -1，排序的时候排到最后～
        similarity[source_index] = -1
        similarity = sorted(enumerate(similarity), key=lambda x: x[1], reverse=True)[:10]
        yield source_index, similarity


class FileReader(metaclass=Singleton):
    """
    中文分词，做 TF-IDF 特种提取
    """

    def __init__(
            self,
            file: str = Configs.FILES.track_path,
    ):
        self.file = file
        self.lock = FileLock(os.path.join(Configs.BASE_DIR, "data/file.lock"))

    def read_and_clean(self) -> pd.DataFrame:
        """读取原始歌曲信息的 csv 表
        格式应为:
            id-1,  somewords
            id-2,  somewords
            ...
            id-n,  somewords
        读取数据后会清理无用字符，然后使用 jieba 分词库将 details 字段分成使用 " " 连接的字符串
        并添加到 clean 的字段上
        将处理完的数据保存到 'clean_tracks.csv' 文件里
        return:
            id, details, clean
            1,  somewords, some words
            2,  somewords, some words
            ...
            n,  somewords, some words
        """
        with self.lock:
            items = pd.read_csv(self.file, names=["id", "details"])

        def _clean(line):
            """清理 detail 字段的无用字符，只保留英文、数字和中文
            """
            line = str(line).strip()
            rule = re.compile("[^a-zA-Z0-9\u4E00-\u9FA5]")
            line = rule.sub("", line)
            return " ".join(jieba.cut(line))

        items["clean"] = items.details.apply(_clean)
        return items

    def write_to_csv(self, data: typing.List[TrackModel]):
        """
        写入 csv 的后台任务
        """
        logging.info(f"{time.strftime('%X')}-> 开始写入 csv 任务, 共 {len(data)} 条数据...")
        self.lock.timeout = -1
        with self.lock:
            with open(self.file, "w") as fw:
                csv_writer = csv.writer(fw)
                for track in data:
                    details = track.artist + track.desc + "".join(
                        tag.name for tag in track.tags if tag.name is not None)
                    csv_writer.writerow([track.track_id, details])
        logging.info(f"{time.strftime('%X')}-> 写入完成.")


def UpdateSimilarityModel(session: Session, limit: int = 0):
    """
    """
    query_string = """
    select t.id as 'track_id',
           artist,
           `desc`,
           tag.name
    from (select id, artist, `desc` from track {}) t
             left outer join track_tag_rel ttr on t.id = ttr.track_id
             left outer join tag on ttr.tag_id = tag.id
    """
    if limit:
        query = query_string.format(f"limit {limit}")
    else:
        query = query_string.format("")

    query_result = session.execute(query)
    track_group = group_query_result(query_result, group_field="track_id")
    track_model = [TrackModel(**track_list[0], tags=track_list) for track_list in track_group]

    file = FileReader()
    file.write_to_csv(track_model)
    subprocess.run(["python", "-m", "recommender"])

    # 更新索引，更新计算状态，删除文件缓存
    global_vars.init_indices()
    global_vars.CALCULATE_STATUS = False
    linecache.clearcache()
    logging.info("updated.")


if __name__ == '__main__':
    logging.info("run spark task...")
    import findspark

    findspark.init()
    conf = SparkConf().setAppName(Configs.SparkConfig.AppName).setMaster(Configs.SparkConfig.Master)
    sc = SparkContext(conf=conf)

    f = FileReader()

    tracks = f.read_and_clean()
    w2v = get_features(tracks)

    indices = pd.Series(tracks.index, index=tracks["id"])
    indices.to_csv(Configs.FILES.indices_path, header=False)

    a_mat = parallelize_matrix(w2v)
    b_mat = broadcast_matrix(w2v)

    result = a_mat.flatMap(
        lambda sub_matrix: find_matches_in_sub_matrix(
            csr_matrix(sub_matrix[1], shape=sub_matrix[2]),
            b_mat,
            sub_matrix[0]
        )
    )
    result = result.collect()
    sc.stop()
    logging.info("spark task done.")

    logging.info("writing the similarity model to file...")
    with open(Configs.FILES.similarity_model, "w") as f:
        for r in result:
            f.write(f"{r[1]}\n")
