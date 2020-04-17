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

fill_id = -1
thickener_id = 1
if thickener_id != 0:
    backfill_record = col_5.find({"thickener_id": thickener_id})
    for x in backfill_record:
        if x["fill_status"] == 1:  # 只有任务处于运行状态(online)才记录
            fill_id = x["_id"]

# 关闭光标对象
cursor.close()

# 关闭数据库连接
conn.close()
client.close()

