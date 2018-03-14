#!/usr/bin/python
# -*- coding: utf-8 -*-


import json
import pymysql
import re
import logging
import sys


def connect_staging_db():
    return pymysql.connect(host='10.10.10.200',
                           port=33061,
                           user='root',
                           password='Cisco123',
                           database='sp2p6',
                           charset='utf8')


def query_by_id(ids):
    sql_str = ("SELECT id, status, rule_a_report FROM t_bids WHERE id in "
               + ids)
    con = connect_staging_db()
    cur = con.cursor()
    cur.execute(sql_str)
    rows = cur.fetchall()
    cur.close()
    con.close()

    return rows


def mock_data(entities, target_status):
    result_list = []
    try:
        for entity in entities:
            bid_id = entity[0]
            status = entity[1]
            rule_report_json_str = entity[2]
            rule_report_json_dict = json.loads(rule_report_json_str)

            ruleset_list = rule_report_json_dict["report"]
            for ruleset in ruleset_list:
                for rule_category in ruleset["data"]:
                    if rule_category["source"] == '运营商规则':
                        rule_list = rule_category["data"]
                        ll = list(map(lambda x: (list(map(lambda d: str(d).replace('在网时长<365天','在网时长<N天')
                                                          .replace('在网月份<5个月','在网月份<N个月')
                                                          .replace('催收号次数>=2','催收号次数>=N')
                                                          .replace('催收号次数>=4','催收号次数>=N')
                                                          .replace('催收号次数>=8','催收号次数>=N')
                                                          .replace('有效通话次数<=0', '有效通话次数<=N')
                                                          .replace('有效通话记录的天数小于5天', '有效通话记录的天数小于N天')
                                                          .replace('有效通话手机号个数<1', '有效通话手机号个数<N')
                                                          .replace('通话次数低于5次', '通话次数低于N次')
                                                          .replace('有效通话次数>0', '有效通话次数>N')
                                                          .replace('通话记录的天数>=15天', '通话记录的天数>=N天')
                                                          .replace('有效通话手机号个数>=15个', '有效通话手机号个数>=N个')
                                                          .replace('通话次数>=10次', '通话次数>=N次')
                                                          .replace('近一个月有通话记录的手机号个数<10', '近一个月有通话记录的手机号个数<N')
                                                          .replace('凌晨一点到五点通话次数占比>30', '凌晨一点到五点通话次数占比>N')
                                                          .replace('凌晨一点到五点通话次数占比>=5', '凌晨一点到五点通话次数占比>=N')
                                                          , x))), rule_list))
                        rule_category["data"] = ll

                    if rule_category["source"] == 'face++':
                        rule_category["source"] = '人脸识别'
                        rule_category["data"] = list(map(lambda x: (list(map(lambda d: str(d).replace('face++','人脸识别')
                                                                             .replace('facepp','人脸识别'), x))), rule_category["data"]))

                    if rule_category["source"] == '索伦反欺诈报告':
                        rule_category["source"] = '第三方反欺诈报告'
                        ll = []
                        for index, value in enumerate(rule_category["data"]):
                            for item in value:
                                if '葫芦分' in item:
                                    ll.append(list(
                                        map(lambda x: str(x).replace('葫芦分', '分数'),
                                            rule_category["data"][index])))
                                    break
                                if '直接联系人在黑名单中数量' in item:
                                    ll.append(list(
                                        map(lambda x: str(x).replace('直接联系人在黑名单中数量>=3', '直接联系人在黑名单中数量>=N'),
                                            rule_category["data"][index])))
                                    break
                                if '绑定其他手机号个数' in item:
                                    ll.append(list(
                                        map(lambda x: str(x).replace('绑定其他手机号个数>=4', '绑定其他手机号个数>=N')
                                            .replace('绑定其他手机号个数<4', '绑定其他手机号个数<N')
                                            .replace('查询此手机号的机构数<15', '查询此手机号的机构数<N'),
                                            rule_category["data"][index])))
                                    break

                        rule_category["data"] = ll



                        rule_category["data"] = list(map(lambda x: (list(map(lambda d: str(d).replace('葫芦分','分数')
                                                                             .replace('绑定其他手机号个数<4', '绑定其他手机号个数<N')
                                                                             .replace('查询此手机号的机构数<15', '查询此手机号的机构数<N')
                                                                             .replace('消费分期出现次数>=5次', '消费分期出现次数>=N次')
                                                                             .replace('贷出现次数>=3次', '贷出现次数>=N次')
                                                                             .replace('历史查询总次数>=30次', '历史查询总次数>=N次')
                                                                             .replace('历史查询总机构数>=10次', '历史查询总机构数>=N次')
                                                                             .replace('最近7天查询次数>5次', '最近7天查询次数>N次')
                                                                             .replace('最近7天查询机构数>3次', '最近7天查询机构数>N次')
                                                                             .replace('最近14天查询次数>=6次', '最近14天查询次数>=N次')
                                                                             .replace('最近14天查询机构数>=3次', '最近14天查询机构数>=N次')
                                                                             .replace('最近30天查询次数>=10次', '最近30天查询次数>=N次')
                                                                             .replace('最近30天查询机构数>=5次', '最近30天查询机构数>=N次')
                                                                             .replace('绑定其他手机号个数>=4', '绑定其他手机号个数>=N')
                                                                             .replace('绑定的其他手机号 查询此手机号的机构数 >= 8', '绑定的其他手机号 查询此手机号的机构数 >= N')
                                                                             , x))), rule_category["data"]))

                    if rule_category["source"] == '索伦黑名单':
                        rule_category["source"] = '第三方黑名单'

                    if rule_category["source"] == '数美逾期':
                        rule_category["source"] = "第三方逾期名单"
                        ll = []
                        for index,value in enumerate(rule_category["data"]):
                            for item in value:
                                if '最近7天' in item:
                                    ll.append(rule_category["data"][index])
                                    break
                                if 'M3+' in item:
                                    ll.append(rule_category["data"][index])
                                    break
                                if '命中灰名单' in item:
                                    ll.append(rule_category["data"][index])
                                    break
                                if '该用户在多少不同网贷平台发生了逾期' in item:
                                    ll.append(list(map(lambda x: str(x).replace('该用户在多少不同网贷平台发生了逾期>=5', '该用户在多少不同网贷平台发生了逾期>=N'), rule_category["data"][index])))
                                    break

                        rule_category["data"] = ll

                    if rule_category["source"] == '数美中介包装风险':
                        rule_category["source"] = '第三方中介包装风险'
                        rule_category["data"] = list(map(lambda x: (list(map(lambda d: str(d).replace('中介呼入次数>=5','中介呼入次数>=N')
                                                                             .replace('中介呼入累计时长>=100s','中介呼入累计时长>=Ns')
                                                                             .replace('呼出到中介的次数>=3','呼出到中介的次数>=N')
                                                                             .replace('呼出到中介累计时长>=60s','呼出到中介累计时长>=Ns')
                                                                             .replace('通讯录包含中介的个数>=5','通讯录包含中介的个数>=N')
                                                                             , x))), rule_category["data"]))

                    if rule_category["source"] == '数美号码标签':
                        rule_category["source"] = '第三方号码标签'

                    if rule_category["source"] == '白骑士黑名单':
                        rule_category["source"] = '第三方黑名单2'
                        rule_category["data"] = list(map(lambda x: (list(map(lambda d: str(d).replace('白骑士',''), x))), rule_category["data"]))

                    if rule_category["source"] == '数美黑名单':
                        rule_category["source"] = '第三方黑名单3'

                    if rule_category["source"] == '数美多平台借贷':
                        rule_category["source"] = '多平台借贷'
                        ll = []
                        for index, value in enumerate(rule_category["data"]):
                            for item in value:
                                if '90天内被>=2家网贷平台拒绝' in item:
                                    ll.append(list(
                                        map(lambda x: str(x).replace('90天内被>=2家网贷平台拒绝', '90天内被>=N家网贷平台拒绝'),
                                            rule_category["data"][index])))
                                    break
                                if '30天内被>=1家网贷平台拒绝' in item:
                                    ll.append(list(
                                        map(lambda x: str(x).replace('30天内被>=1家网贷平台拒绝', '30天内被>=N家网贷平台拒绝'),
                                            rule_category["data"][index])))
                                    break

                        rule_category["data"] = ll

                    if rule_category["source"] == '进件次数':
                        rule_category["data"] = list(map(lambda x: (list(map(lambda d: str(d).replace('进件次数 > 3','进件次数 > N'), x))), rule_category["data"]))

                    if rule_category["source"] == '芝麻信用分':
                        rule_category["data"] = list(map(lambda x: (list(map(lambda d: str(d).replace('芝麻分低于580分','芝麻分低于X分'), x))), rule_category["data"]))

                    if rule_category["source"] == '排列模型分':
                        rule_category["data"] = list(map(lambda x: (list(map(lambda d: str(d).replace('排列V2模型分<560分','排列V2模型分<X分')
                                                                             .replace('排列分低于450分', '排列分低于X分')
                                                                             .replace('排列个人素质模型分低于500分', '排列个人素质模型分低于X分')
                                                                             , x))), rule_category["data"]))

                    if rule_category["source"] == '芝麻分&排列V2':
                        rule_category["data"] = list(map(lambda x: (list(map(lambda d: str(d).replace('芝麻分<600分，且排列V2模型分<600分','芝麻分<X分，且排列V2模型分<X分'), x))), rule_category["data"]))

                    if rule_category["source"] == '芝麻分&运营商&排列V2':
                        rule_category["data"] = list(map(lambda x: (list(map(lambda d: str(d).replace('（（在网时长>=365天, 且在网时长<500天）或者 电信爬取 >=5个月）, 且排列V2模型分<600分','（（在网时长>=N天, 且在网时长<N天）或者 电信爬取 >=N个月）, 且排列V2模型分<X分')
                                                                             .replace('（在网时长>=500天，或者电信爬取>=5个月）, 且600<=芝麻分<630，且排列V2模型分<580分', '（在网时长>=N天，或者电信爬取>=N个月）, 且X<=芝麻分<X，且排列V2模型分<X分'), x))), rule_category["data"]))

                    if rule_category["source"] == '相同IP':
                        rule_category["data"] = list(map(lambda x: (list(map(lambda d: str(d).replace('24小时内同一IP申请次数<20次','24小时内同一IP申请次数<N次')
                                                                             .replace('24小时内同一IP申请次数>=20次', '24小时内同一IP申请次数>=N次'), x))), rule_category["data"]))

                    if rule_category["source"] == '通过人数限制':
                        rule_category["data"] = list(map(lambda x: (list(map(lambda d: str(d).replace('通过规则A2自动放款人数<=300','通过规则A2自动放款人数<=N'), x))), rule_category["data"]))

                    if rule_category["source"] == '运营商附加C规则':
                        ll = []
                        for index,value in enumerate(rule_category["data"]):
                            for item in value:
                                if '近三月联系人top10的list' in item:
                                    ll.append(rule_category["data"][index])
                                    break
                                if '亲属联系人不在top10的list中' in item:
                                    ll.append(rule_category["data"][index])
                                    break
                                if '朋友圈主要活跃区域' in item:
                                    ll.append(rule_category["data"][index])
                                    break
                                if '手机关机天数' in item:
                                    ll.append(rule_category["data"][index])
                                    break
                                if '与贷款号码联系情况' in item:
                                    ll.append(rule_category["data"][index])
                                    break
                                if '近1月联系人手机号命中催收号次数' in item:
                                    ll.append(list(map(lambda x: str(x).replace('近1月联系人手机号命中催收号次数>=1', '近1月联系人手机号命中催收号次数>=N'), rule_category["data"][index])))
                                    break
                                if '漫游的城市数量' in item:
                                    ll.append(list(map(lambda x: str(x).replace('漫游的城市数量>=5', '漫游的城市数量>=N'), rule_category["data"][index])))
                                    break
                                if '近6月联系人手机号命中澳门地区次数' in item:
                                    ll.append(list(map(lambda x: str(x).replace('近6月联系人手机号命中澳门地区次数>0', '近6月联系人手机号命中澳门地区次数>N'), rule_category["data"][index])))
                                    break

                        rule_category["data"] = ll

                    print 666
            rule_report_json_dict["report"] = ruleset_list
            update_rule_report(bid_id, str(json.dumps(rule_report_json_dict).encode('utf8').decode('unicode_escape')), target_status)
            print entity[0]
    except Exception as e:
        logging.error('edit failed!!' ,e)
    finally:
        return entities


def update_rule_report(bid_id, report_str, target_status):
    sql_update_str = "UPDATE t_bids SET status = '%i', rule_a_report = '%s' WHERE id = %d"
    con = connect_staging_db()
    cur = con.cursor()
    try:
        cur.execute(sql_update_str % (target_status, report_str, bid_id))
        con.commit()
    except Exception as e:
        con.rollback()
    finally:
        cur.close()
        con.close()

if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding("utf-8")

    # ids = '(7612, 7615, 7623, 7605, 7634, 7569)'
    ids = '(7623, 7605)'

    entities = query_by_id(ids)
    print mock_data(entities, 0)
    # print mock_data(entities, 2)
