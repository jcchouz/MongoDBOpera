# -*- coding: UTF-8 -*-

# 存储所有设备信息

import pymongo
import xlrd
import datetime

"""点位信息表
point_id: 点位编号
opc_tag: 点位tag
describe: 点位描述
device_description_chinese: 点位描述中文
serial: 点位所属仪器编号
instrument: 点位所属仪器
device_name_chinese: 点位所属仪器中文
belong_co: 点位所属公司
status: 点位状态
value_min: 正常工作最小值
value_max: 正常工作最大值
unit: 单位
last_monitor_time: 最近数据采集时间
"""

# 建立连接
client = pymongo.MongoClient("mongodb://202.204.54.23:27017/nfca_db", username='nfca', password='nfca')
db = client["nfca_db"]
col = db["point"]


def insert_json_data(point_id, opc_tag, describe, device_description_chinese, serial, instrument, device_name_chinese, belong_co, status, value_min, value_max, unit, last_monitor_time):
    data = {
        "point_id": point_id,
        "opc_tag": opc_tag,
        "describe": describe,
        "device_description_chinese": device_description_chinese,
        "serial": serial,
        "instrument": instrument,
        "device_name_chinese": device_name_chinese,
        "belong_co": belong_co,
        "status": status,
        "value_min": value_min,
        "value_max": value_max,
        "unit": unit,
        "last_monitor_time": last_monitor_time
    }
    result = col.insert_one(data)
    print(result.acknowledged)  # 判断有没有插入成功，成功True，失败：False


if __name__ == '__main__':

    ins_info = xlrd.open_workbook("./point.xlsx")
    sheet = ins_info.sheet_by_index(0)  # 表
    nrows = sheet.nrows  # 行数
    print(nrows)
    ncols = sheet.ncols  # 列数
    point_id = 1
    for rownum in range(nrows):
        row = sheet.row_values(rownum)
        if row:
            insert_json_data(
                point_id=point_id,
                opc_tag=row[1],
                describe=row[2],
                device_description_chinese=row[3],
                serial=row[0],
                instrument=row[4],
                device_name_chinese=row[5],
                belong_co="METSO",
                status="normal",
                value_min=row[6],
                value_max=row[7],
                unit=row[8],
                last_monitor_time=datetime.datetime.strptime("2019-11-25 12:24:40", "%Y-%m-%d %H:%M:%S")
            )
        point_id += 1
