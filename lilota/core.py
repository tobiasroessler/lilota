from lilota.scheduler import LilotaScheduler
from lilota.worker import LilotaWorker


class Lilota():

  def __init__(self, db_url: str):
    self._db_url = db_url
    self._scheduler = LilotaScheduler(db_url)