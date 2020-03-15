# -*- coding: UTF-8 -*-

# 连接李佳的MySQL数据库，读取数据

import pymongo
import pymysql
import datetime

'''
"ins_id": 设备号，根据设备号查找设备
"time": 监测时间
"value": 监测值
"status": 状态，暂定都为True
'''

nowTime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # 现在时间
print("========" + str(nowTime) + "========")

# 建立MySQL连接
conn = pymysql.connect(
    host="202.204.54.23",
    port=3308,
    user="root",
    password="admin",
    db="buffer",
    charset="utf8",
    autocommit=True
)

# 建立Mongodb连接
client = pymongo.MongoClient(
    "mongodb://202.204.54.23:27017/nfca_db",
    username='nfca',
    password='nfca'
)
db = client["nfca_db"]
col_1 = db["point"]
col_2 = db["gms_monitor"]
col_3 = db["warning_log"]
col_4 = db["gms_now"]
col_5 = db['backfill_record']

# 得到一个可以执行SQL语句的光标对象
cursor = conn.cursor()

# 定义要执行的SQL语句
sql_1 = "select * from gms_monitor"

# 执行SQL语句
cursor.execute(sql_1)
result = cursor.fetchall()

# 先清空gms_now表，即现在的监测值，再更新数据
col_4.remove()

# 根据tag,找到对应id,再插入数据
for r in result:
    point = col_1.find_one({"opc_tag": r[1]})
    id = point["_id"]
    print(id)
    point_id = point["point_id"]
    instrument = point["instrument"]
    value_min = point["value_min"]
    value_max = point["value_max"]
    alarm = False  # 默认数据处在正常范围内
    monitoring_value = r[3]
    time = r[2]
    state = r[4]
    print(state)

    # 根据point_id找点位，找到点位对应的浓密机号thickener_id 或者 搅拌机号mixer_id，根据thickener_id/mixer_id找对应的充填任务，得到对应的fill_id
    # 即point_id --> thickener_id/mixer_id --> fii_id
    thickener_id = point['thickener_id']
    if thickener_id != 0:
        backfill_record = col_5.find_one({"thickener_id": thickener_id})
        if backfill_record:
            if backfill_record["fill_status"] == 1:  # 只有任务处于运行状态(online)才记录
                fill_id = backfill_record["_id"]
            else:
                fill_id = -1
        else:
            fill_id = -1
    else:
        mixer_id = point['mixer_id']
        backfill_record = col_5.find_one({"mixer_id": mixer_id})
        if backfill_record:
            if backfill_record["fill_status"] == 1:  # 只有任务处于运行状态(online)才记录
                fill_id = backfill_record["_id"]
            else:
                fill_id = -1
        else:
            fill_id = -1

    # 如果是泵，原始监测值会是很长一串，则将泵的原始监测值转换为0/1
    if point["instrument"] == "Valve":
        monitoring_value = (int(monitoring_value) >> 20) & 1

    if monitoring_value < value_min or monitoring_value > value_max:  # 数据超出正常范围就记录到报警日志表
        warning = {
            "point_id": point_id,
            "instrument": instrument,
            "time": datetime.datetime.strptime(str(time), '%Y-%m-%d %H:%M:%S'),
            "principal": 1,
            "value_min": value_min,
            "value_max": value_max,
            "Monitoring_value": monitoring_value,
            "fill_id": fill_id
        }
        col_3.insert_one(warning)
        alarm = True
        print(str(r[2]) + "  数据异常，插入报警日志表.")

    # 插入数据到历史数据表
    monitor = {
        "point_id": point_id,
        "instrument": instrument,
        "time": datetime.datetime.strptime(str(time), '%Y-%m-%d %H:%M:%S'),
        "Monitoring_value": monitoring_value,
        "alarm": alarm,
        "point": id,
        "fill_id": fill_id
    }
    col_2.insert_one(monitor)

    # 更新实时监测值数据表的数据
    gms_now_query = col_4.find_one({"point_id": point_id})
    if gms_now_query is None:
        col_4.insert_one(monitor)
    else:
        newvalues = {"$set":
                         {
                             "instrument": instrument,
                             "time": datetime.datetime.strptime(str(time), '%Y-%m-%d %H:%M:%S'),
                             "Monitoring_value": monitoring_value,
                             "alarm": alarm,
                             "state": state,
                             "fill_id": fill_id
                          }
                     }
        col_4.update_one(gms_now_query, newvalues)

    # 删除buffer中的数据
    sql_2 = "DELETE FROM gms_monitor WHERE id = %d" % r[0]
    cursor.execute(sql_2)
    print(str(r[2]) + "  monitor数据插入并删除成功.")

# 关闭光标对象
cursor.close()

# 关闭数据库连接
conn.close()
client.close()

