#!/usr/bin/python
# -*- coding:utf8 -*-
import pymongo
import dateutil.parser

dateStartStr = '2021-04-07T16:06:48Z'
dateEndStr= '2021-04-09T06:49:34Z'
dateStartPar = dateutil.parser.parse(dateStartStr)
dateEndPar = dateutil.parser.parse(dateEndStr)
# 建立连接
client = pymongo.MongoClient(
    "mongodb://127.0.0.1:27017/nfca_db",
    username='nfca',
    password='nfca'
)
db = client["nfca_db"]
myresult = db.gms_monitor.find({ "time" : { "$gte" : dateStartPar, "$lte" : dateEndPar } })
# 总记录条数
print(myresult.count())
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
            'point', monitor['point']
        ).tag(
            'alarm', monitor['alarm']
        ).tag(
            'fill_id', monitor['fill_id']
        ).field(
            'Monitoring_value', float(monitor['Monitoring_value'])
        ).field(
            'instrument', monitor['instrument']
        ).time(
            datetime.datetime.strptime(str(monitor['time']), '%Y-%m-%d %H:%M:%S')
        )
    )


#  批量写入influxdb
write2influx_db(database_config['influxdb'], influxdb_write_sequence, logger)