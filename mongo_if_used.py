# -*- coding: UTF-8 -*-

# 连接李佳的MySQL数据库，读取数据

import pymongo
import pymysql
import datetime

'''
"ins_id": 设备号，根据设备号查找设备
"time": 监测时间
"if_used": 控制信号，是否使用
'''

nowTime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # 现在时间
print("========" + str(nowTime) + "========")

# 建立MySQL连接
conn = pymysql.connect(
    host="202.204.62.229",
    port=3306,
    user="root",
    password="admin",
    db="buffer",
    charset="utf8",
    autocommit=True
)

# 建立Mongodb连接
client = pymongo.MongoClient(
    "mongodb://202.204.62.229:27017/nfca_db",
    username='nfca',
    password='nfca'
)
db = client["nfca_db"]
col_1 = db["point"]
col_2 = db["gms_if_used"]

# 得到一个可以执行SQL语句的光标对象
cursor = conn.cursor()

# 定义要执行的SQL语句
sql_1 = "select * from gms_if_used"


# 执行SQL语句
cursor.execute(sql_1)
result = cursor.fetchall()
# 根据tag,找到对应id,再插入数据
for r in result:
    x = col_1.find_one({"opc_tag": r[1]})
    monitor = {
        "point_id": x["point_id"],
        "time": datetime.datetime.strptime(str(r[2])[0:19], '%Y-%m-%d %H:%M:%S'),
        "adoption": True if r[3] == 1 else False
    }
    col_2.insert_one(monitor)
    sql_2 = "DELETE FROM gms_if_used WHERE id = %d" % r[0]
    cursor.execute(sql_2)
    print(str(r[2]) + "  if_used数据插入并删除成功.")

# 关闭光标对象
cursor.close()

# 关闭数据库连接
conn.close()
client.close()

