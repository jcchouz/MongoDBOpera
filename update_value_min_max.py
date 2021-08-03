# -*- coding: UTF-8 -*-


import pymongo
import xlrd
import datetime

"""点位信息表
value_min: 正常工作最小值
value_max: 正常工作最大值
"""

# 建立连接
client = pymongo.MongoClient("mongodb://127.0.0.1:27017/nfca_db", username='nfca', password='nfca')
db = client["nfca_db"]
col = db["point"]

point_data = xlrd.open_workbook("./point-2021-08-03(update_value_min_max).xlsx")
sheet = point_data.sheet_by_index(0)  # 表
nrows = sheet.nrows  # 行数
point_id = 1
for rownum in range(nrows):
    row = sheet.row_values(rownum)
    if row:
        # 修改一条数据 update_one()方法 ,如果查找到的匹配数据多于一条，则只会修改第一条。
        myquery = {"point_id": point_id}
        newvalues = {"$set":
                        {
                            "value_min": row[10],
                            "value_max": row[11],
                            "value_max_2": row[16],
                            "value_min_2": row[17]
                        }
                    }
        col.update_one(myquery, newvalues)  # 更新值

    point_id = point_id + 1


