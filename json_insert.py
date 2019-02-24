import pymongo
import json
from datetime import datetime
import time

# 建立连接
myclient = pymongo.MongoClient("mongodb://202.204.62.145:27017/", username='admin', password='afish1001')
mydb = myclient["runoobdb"]
mycol = mydb["json_data"]


def insert_json_data():
    file = open('./2019-02-21-10-19.txt', 'r', encoding="utf-8")
    for each in file:
        print(each)
        eachline = json.loads(each)
        time = datetime.strptime(eachline["time"], "%Y-%m-%d %H:%M:%S") # 将时间类型转换：str -> date
        data = {"monitor_id": eachline["monitor_id"], "display": eachline["display"], "time": time}
        result = mycol.insert_one(data)
        print(result.acknowledged)  # 判断有没有插入成功，成功True，失败：False
    file.close()


if __name__ == '__main__':
    # mycol.drop()
    # insert_json_data()
    # 查询总条数
    # print(mycol.find().count())
    myresult = mycol.find()
    objectid = myresult[0]["_id"]  # myresult[0]:第一条记录
    print("objectid: %s" % objectid)  # _id
    timestamp = time.mktime(objectid.generation_time.timetuple())
    print("timestamp: %d" % timestamp)
    timeArray = time.localtime(timestamp)
    otherStyleTime = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)
    print("time_transfer: %sUTC" % otherStyleTime)
