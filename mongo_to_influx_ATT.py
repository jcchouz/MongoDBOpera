#!/usr/bin/python
# -*- coding:utf8 -*-

"""
Author: Yuanzhaolin
Date: 2021年2月1日

功能: 读取mysql数据录入mongodb和influxdb
使用方法:
python data_import.py [--debug] --mode [nfca|ustb]
本校区内采用ustb，非矿现场设置为nfca

2021.04.09 周佳城 从mongo读数据到influxdb，according to time(ATT)
"""

import sys, os, pymysql
import yaml
import getopt
import time
import datetime
import pymongo
for _ in range(1, 4):
    sys.path.append('../' * _)  # 便于直接在目录下运行该文件
from nfcadb.buffer_to_nfcadb.point_cache import InstrumentPointCache
from nfcadb.buffer_to_nfcadb.database_util import MysqlUtil, write2influx_db
from influxdb_client import InfluxDBClient, WritePrecision
from influxdb_client import Point as InfluxdbPoint
import dateutil.parser


class Logger:
    def __init__(self, logger_path, debug):
        """

        :param logger_path:
        :param level: debug or error
        """

        self.debug = debug
        self.logger_path = logger_path

    def log(self, level, msg):
        if level == 'debug' and not self.debug:
            pass
        else:
            with open(self.logger_path, 'a+') as f:
                f.write('[{}] : {} | {}'.format(str(level).upper(), str(datetime.datetime.now()), msg)+'\n')
            print('[{}] : {} | {}'.format(str(level).upper(), str(datetime.datetime.now()), msg))

    def __call__(self, *args, **kwargs):
        return self.log(*args, **kwargs)


def read_latest_databatch(mysql_config: dict, logger, num_limit: int):
    """
    返回mysql数据，上限为num_limit条。
    mysql连接异常在MysqlUtil中捕获
    :param mysql_config: dict
    :param logger:
    :param num_limit:
    :return:
    """
    mysql_util = MysqlUtil(mysql_config, logger)
    if mysql_util.conn is None:
        return None
    # 得到一个可以执行SQL语句的光标对象

    cmd = f'select * from gms_monitor limit {num_limit}'

    result = mysql_util.handle_command(cmd)
    if result is None:
        logger.log('error', 'Mysql检索失败')
    else:
        logger.log('log', 'Mysql检索成功，共{}条'.format(len(result)))
    mysql_util.close()
    return result


def delete_mysql_data_batch(mysql_config, ids, logger):

    mysql_util = MysqlUtil(mysql_config, logger)
    if mysql_util.conn is None:
        return None
    # 得到一个可以执行SQL语句的光标对象

    # 执行SQL语句
    # 定义要执行的SQL语句

    # 用于删除的SQL，sql命令最长估计不超过1m，mysql限制的最大sql commond长度大约是4m
    cmd = 'DELETE FROM gms_monitor WHERE id IN ({})'.format(
        ','.join(map(str, ids))
    )
    result = mysql_util.handle_command(cmd)
    if result is None:
        logger.log('error', 'Mysql数据删除失败')
        return None
    else:
        logger.log('log', 'Mysql数据删除成功，共{}条'.format(len(ids)))
    mysql_util.close()
    return result


def collect_mongodb_docs(mongodb_config: dict, logger):
    """
    获取'point', 'gms_monitor', 'warning_log', 'gms_now', 'backfill_record'五个doc
    :param mongodb_config:
    :param logger:
    :return:
    """
    try:
        client = pymongo.MongoClient(
            mongodb_config['uri'],
            username=mongodb_config['username'],
            password=mongodb_config['password']
        )
        logger.log('log', "连接Mongodb数据库成功")
    except Exception as e:
        logger.log('error', str(e) +  '\n' + '连接Mongodb数据库失败')
        return None, None
    db = client["nfca_db"]

    try:
        docs = tuple(
            [
                db[doc_name] for doc_name in ['point', 'gms_monitor', 'warning_log', 'gms_now', 'backfill_record']
            ]
        )
    except Exception as e:
        logger.log('error', str(e) +  '\n' + '读取Mongodb 文档失败')
        return None, None
    return docs, client


def find_backfill_id(point, monitoring_time, mongodb_backfilling_doc, logger):
    """

    :param point: 监测数据对应的点位
    :param monitoring_time: 监测数据时间
    :param mongodb_backfilling_doc:
    :param logger:
    :return:
    """

    if point['thickener_id'] != 0:
        instrument_filter = {'thickener_id': int(point['thickener_id'])}
    else:
        instrument_filter = {'mixer_id': int(point['mixer_id'])}

    """ 
    检索规则解释：
    加入数据录入服务断线了一段时间，这会导致mysql中的数据不断累积。重启数据录入服务，将当前充填任务的fill_id写入历史数据是不合理的
    因此检索满足如下条件的充填任务:
    1. 监测时间夹在充填任务的起止时间之间 或  监测时间在充填任务的起始时间之后且该任务无结束时间
    2. 满足instrument_filter
    """
    backfill_task_filter = dict(
        {
            "$or":
                [
                    {'start_time': {'$lt': monitoring_time}, 'end_time': {'$gte': monitoring_time}},
                    {'start_time': {'$lt': monitoring_time}, 'end_time': None}
                ]
        }, **instrument_filter
    )
    backfilling_belong = mongodb_backfilling_doc.find(
        backfill_task_filter
    ).sort([('start_time', -1)])

    if backfilling_belong.count()>1:
        # 理论上不存在两个任务同时满足该检索条件
        logger.log(
            'warning', 'Backfilling missions {} share {} {}'.format(
                ','.join(str([x['fill_id'] for x in backfilling_belong])),
                'mixer' if point['thickener_id']!=0 else 'thickener',
                point['mixer_id'] if point['thickener_id']!=0 else point['thickener_id']
            )
        )
    fill_id = -1

    # 研究很久没找到怎么取返回结果集里的第一个，用了看起来最蠢的方法
    for backfilling in backfilling_belong:
        fill_id = backfilling['fill_id']
        # 因为检索条件里按照时间逆序，所以只需要取第一个
        break
    return fill_id


def read_yaml(yaml_path='./database_config.yaml', mode='ustb'):

    if not os.path.exists(yaml_path):
        raise FileNotFoundError('无法在当前路径找到database_config.yaml')
    f = open(yaml_path, 'r', encoding='utf-8')
    yaml_config = yaml.load(f.read(), Loader=yaml.FullLoader)
    return yaml_config


def main(debug, mode):

    logger = Logger('./db_import_log.log', debug)
    logger('debug', 'test')
    yaml_config = read_yaml(mode=mode)
    database_config = yaml_config[mode]

    dateStartStr = '2021-04-02T11:40:48Z'
    # dateEndStr= '2021-04-09T06:49:34Z'
    dateStartPar = dateutil.parser.parse(dateStartStr)
    # dateEndPar = dateutil.parser.parse(dateEndStr)
    # 建立连接
    client = pymongo.MongoClient(
        "mongodb://127.0.0.1:27017/nfca_db",
        username='nfca',
        password='nfca'
    )
    db = client["nfca_db"]
    # myresult = db.gms_monitor.find({ "time" : { "$gte" : dateStartPar, "$lte" : dateEndPar } })
    myresult = db.gms_monitor.find({ "time" : { "$gte" : dateStartPar} })
    # 总记录条数
    print(myresult.count())
    influxdb_write_sequence = []
    for monitor in myresult:
        # 构造influxdb数据点并加入列表
        # 存储形式
        # measurement: gms_monitor
        # tag: point, point_id, alarm, fill_id
        # field, Monitoring_value, instrument
        influxdb_write_sequence.append(
            InfluxdbPoint(
                'gms_monitor'
            ).tag(
                'point_id', monitor['point_id']
            ).tag(
                'fill_id', monitor['fill_id']
            ).field(
                'alarm', monitor['alarm']
            ).field(
                'Monitoring_value', float(monitor['Monitoring_value'])
            ).time(
                datetime.datetime.strptime(str(monitor['time']), '%Y-%m-%d %H:%M:%S')
            )
        )
        

    #  批量写入influxdb
    write2influx_db(database_config['influxdb'], influxdb_write_sequence, logger)

    print('done.')


if __name__ == '__main__':
    debug = False
    mode = None
    try:
        opts, args = getopt.getopt(sys.argv[1:], "", ["debug", "mode="])
        for o, a in opts:
            if o == '--debug':
                debug = True
            elif o == '--mode' and (a == 'nfca' or a == 'ustb'):
                mode = a
            else:
                print('python data_import.py [--debug] --mode [nfca|ustb]')
                raise getopt.GetoptError(msg='Illegal opt or argv')
        if mode is None:
            print('python data_import.py [--debug] --mode [nfca|ustb]')
            raise getopt.GetoptError('Mode missed')
    except getopt.GetoptError as e:
        raise e

    main(debug, mode)


