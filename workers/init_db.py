import worker
from database import tables
from database import db_utils
from api import config

class DatabaseInitWorker(worker.Worker):
    def __init__(self):
        con = db_utils.Connect(use_db=False)
        con.query("drop database if exists sarahlytics")
        con.query("create database sarahlytics")
        super(DatabaseInitWorker, self).__init__()

    def DoWorkInternal(self):
        self.tables.Create(self.con)

class PrepopulateWorker(worker.Worker):
    def __init__(self):
        super(PrepopulateWorker, self).__init__()

    def DoWorkInternal(self):
        self.tables.channels.Insert(self.con, [config.my_channel])
