__all__ = ["global_vars"]

import pandas as pd

from configs import Configs


class GlobalVars(object):
    """
    定义了三个（伪）全局变量
    CALCULATE_STATUS: 计算状态（bool），如果为 True，表示当前正在计算物品相似度，不再接受其他的计算请求
                      计算结束后会改为 False

    INDICES: 歌曲索引，通过索引获取歌曲 id
    REVERSED_INDICES: 反向索引， 通过歌曲 id 获取索引
    """

    def __init__(self):
        self._CALCULATE_STATUS = False
        self._INDICES = None
        self._REVERSED_INDICES = None
        self._DATA = None

    @property
    def CALCULATE_STATUS(self):
        return self._CALCULATE_STATUS

    @CALCULATE_STATUS.setter
    def CALCULATE_STATUS(self, status: bool):
        self._CALCULATE_STATUS = status

    @property
    def INDICES(self):
        return self._INDICES

    @property
    def REVERSED_INDICES(self):
        return self._REVERSED_INDICES

    def init_indices(self):
        self._INDICES = pd.read_csv(Configs.FILES.indices_path, names=["track_id", "index"])
        self._REVERSED_INDICES = pd.Series(self._INDICES["index"].values, index=self._INDICES["track_id"])

    @property
    def DATA(self):
        if self._DATA is None:
            self._DATA = pd.read_csv(Configs.FILES.track_path, names=["track_id", "description"])
        return self._DATA


global_vars = GlobalVars()
