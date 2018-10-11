# -*- coding: utf-8 -*-
import urllib.request
import time
import schedule
import json
import os
from bs4 import BeautifulSoup
import threading
import logging
import sys
from logging.handlers import TimedRotatingFileHandler
import random

# 获取logger实例，如果参数为空则返回root logger
logger = logging.getLogger()

# 指定logger输出格式
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# 日志滚动，按照设置的时间删除日志，文件日志
file_handler = TimedRotatingFileHandler(filename='D:/puf-etfms/console.log', when="D", interval=1,
                                        backupCount=3, encoding='utf-8')
file_handler.suffix = "%Y-%m-%d.log"

# 普通方法控制器 file_handler = logging.FileHandler("C:/Users/An/Desktop/python/data/test.log",'w','utf-8')
file_handler.setFormatter(formatter)  # 可以通过setFormatter指定输出格式

# 控制台日志
console_handler = logging.StreamHandler(sys.stdout)
console_handler.formatter = formatter  # 也可以直接给formatter赋值

# 为logger添加的日志处理器，可以自定义日志处理器让其输出到其他地方
logger.addHandler(file_handler)
logger.addHandler(console_handler)

# 指定日志的最低输出级别，默认为WARN级别
logger.setLevel(logging.INFO)

# 上交所主页
host = "http://www.sse.com.cn/"

# 平安大华抓取数据的url
# daHua_url = 'http://fund.pingan.com/main/peanutFinance/yingPeanut/fundDetail/510390.shtml'
# 获取数据的js

# cookies
cookie = "yfx_c_g_u_id_10000042=_ck18012400553818600033745351315; VISITED_BOND_CODE=%5B%22018005%22%2C%22010107%22%5D; seecookie=%5B010107%5D%3A21%u56FD%u503A%u247A; VISITED_COMPANY_CODE=%5B%22018005%22%2C%22010107%22%2C%22510050%22%2C%22510010%22%5D; VISITED_FUND_CODE=%5B%22510050%22%2C%22510010%22%5D; VISITED_MENU=%5B%228550%22%2C%228542%22%2C%228451%22%2C%228487%22%2C%229692%22%2C%228491%22%2C%228312%22%2C%228362%22%2C%228361%22%2C%2210848%22%2C%228363%22%5D; yfx_f_l_v_t_10000042=f_t_1516726538855__r_t_1516777113896__v_t_1516800409603__r_c_2"
# 上交所请求头信息
header1 = {
    'User-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36',
    'Cookie': cookie,
    'Connection': 'keep-alive',
    'Accept': '*/*',
    'Accept-Encoding': 'gzip, deflate, sdch',
    'Accept-Language': 'zh-CN,zh;q=0.8',
    'Host': 'query.sse.com.cn',
    'Referer': 'http://www.sse.com.cn/disclosure/fund/etflist/detail.shtml?type=8&etfClass=2'
}
# 平安大华的请求头
header2 = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate',
    'Accept-Language': 'zh-CN,zh;q=0.9',
    'Cache-Control': 'max-age=0',
    'Connection': 'keep-alive',
    'Cookie': 'BIGipServerpuf-nws_ng_http_DMZ_PrdPool=577838252.4984.0000; BIGipServerpuf-nws_http_DMZ_PrdPool=393288876.3704.0000; WEBTRENDS_ID=4.0.4.35-696938240.30653435; dahuaWebSessionId=6XUnbCvTa0BUGab7k8Ky3sKlQoj2xitCxCef-JIs-WL3dZ_joNSw!-129132870; WT-FPC=id=4.0.4.35-696938240.30653435:lv=1521079844521:ss=1521079824599:fs=1521076554013:pn=2:vn=2',
    'Host': 'fund.pingan.com',
    'Referer': 'http://fund.pingan.com/main/peanutFinance/yingPeanut/index.shtml',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36'
}
file_path = "D:/puf-etfms"


# 获取数据
def get_html(url, header):
    req = urllib.request.Request(url, None, header)
    html = urllib.request.urlopen(req).read()
    return html.decode()


# 获取数据，网页编码为gbk
def get_html_gbk(url, header):
    req = urllib.request.Request(url, None, header)
    html = urllib.request.urlopen(req).read()
    return html.decode('gbk')


fund_code1 = "000300"
fund_code2 = "000905"
# 要获取的基金对应的type类型(000300)
data_dict1 = {"510301": "8", "510311": "33", "510331": "30", "510361": "88", "510391": "133"}
# (000905)
data_dict2 = {"510501": "31", "510511": "41", "510521": "60", "510561": "83", "510581": "89", "510591": "138"}

# 上交所下载
data_down = {"510391": "133", "510591": "138", "512391": "147", "512361": "146"}

# 上交所页面下载
data_down1 = {"SS_510391": "133", "SS_510591": "138", "SS_512391": "147", "SS_512361": "146"}

# 平安大华页面爬取
data_down2 = {"DH_510391": "510390", "DH_510591": "510590", "DH_512391": "512390", "DH_512361": "512360"}

# 平安大华下载
data_down3 = {"510391": "510390", "510591": "510590", "512391": "512390", "512361": "512360", "159961": "159960"}

# 平安大华页面爬取(港股)
data_down4 = {"DH_159961": "159960"}

def mk_dir(code):
    logger.info(u"开始创建文件夹")
    date_path = ''
    if not os.path.isdir(file_path):
        logger.info(u"配置路径不存在，创建文件夹失败，路径%s不存在" % file_path)
    else:
        date = time.strftime("%Y%m%d")
        if code == '1':
            date_path = file_path + "/" + date
        else:
            date_path = file_path + "/" + code + "/" + date
        if not os.path.exists(date_path):
            os.makedirs(date_path)
            logger.info(u"文件夹%s创建成功" % date_path)
        else:
            logger.info(u"文件夹%s已经存在，无需创建" % date_path)
    return date_path


def mk_dir2():
    logger.info(u"开始创建下载文件夹")
    date_path = ''
    if not os.path.isdir(file_path):
        logger.info(u"配置路径不存在，创建文件夹失败，路径%s不存在" % file_path)
    else:
        date = time.strftime("%Y%m%d")
        date_path = file_path + "/" + "download" + "/" + date
        if not os.path.exists(date_path):
            os.makedirs(date_path)
            logger.info(u"文件夹%s创建成功" % date_path)
        else:
            logger.info(u"文件夹%s已经存在，无需创建" % date_path)
    return date_path


# 抓取上交所数据的任务
def job1(code, data_dict):
    # noinspection PyBroadException
    try:
        logger.info(u"[%s]上交所定时任务开始" % code)
        # 创建日期文件夹
        date_path = mk_dir(code)
        for key, value in data_dict.items():
            # 根据基金代码对应的类型，请求申购赎回清单数据的路径，为了获取基金名称
            etf_url = "http://query.sse.com.cn/infodisplay/queryETFBasicInfo.do?isPagination=false&type=" + value + "&market=&etfClass=2&_=15174929" + str(random.randint(10000,99999))
            logger.info(etf_url)
            try:
                # 获取的etf数据
                etf = get_html(etf_url, header1)
                # json转python类型
                etf_data = json.loads(etf)
                fundName = etf_data["result"][0]["fundCompName"]
                # 根据基金代码对应的类型，请求要抓取json数据的路径
                data_url = "http://query.sse.com.cn/infodisplay/queryConstituentStockInfo.do?isPagination=false&type=" + value + "&market=&etfClass=2&_=15168004" + str(random.randint(10000,99999))
                logger.info(data_url)
                # 获取的json数据
                json_data = get_html(data_url, header1)
            except Exception as ex:
                logger.exception(repr(ex))
            # 写入文件
            with open(date_path + "/" + fundName + key + '.txt', "w", encoding='utf-8') as file:
                file.write(json_data)
        logger.info(u"－－－开始休眠10秒－－－－")
        time.sleep(10)
    except Exception as e:
        logger.exception(u"[%s]上交所定时任务出现异常" % code)
        logger.exception(repr(e))
    finally:
        logger.info(u"[%s]上交所定时任务结束" % code)


# 下载上交所数据
def job2(data):
    try:
        logger.info(u"上交所下载定时任务开始")
        # 创建日期文件夹
        date_path = mk_dir2()
        for key, value in data.items():
            etf_url = "http://query.sse.com.cn/etfDownload/downloadETF2Bulletin.do?etfType=" + value
            logger.info(etf_url)
            path = date_path + "/" + key + ".ETF"
            try:
                urllib.request.urlretrieve(etf_url, path)
            except Exception as ex:
                logger.exception(repr(ex))
            logger.info(u"－－－开始休眠10秒－－－－")
            time.sleep(10)
    except Exception as e:
        logger.exception(u"上交所下载定时任务出现异常")
        logger.exception(repr(e))
    finally:
        logger.info(u"上交所下载定时任务结束")


# 抓取上交所页面数据的任务
def job3(data):
        try:
            logger.info(u'抓取上交所页面数据定时任务开始')
            date_path = mk_dir2()
            for key, value in data.items():
                etf_url = "http://query.sse.com.cn/infodisplay/queryETFBasicInfo.do?isPagination=false&type=" + value + "&market=&etfClass=2&_=15257658" + str(random.randint(10000,99999))
                logger.info(etf_url)
                try:
                    # 获取的etf数据
                    header = get_html(etf_url, header1)
                    # json转python类型
                    header_data = json.loads(header)
                    result = header_data["result"][0]
                    # 根据基金代码对应的类型，请求要抓取json数据的路径
                    data_url = "http://query.sse.com.cn/infodisplay/queryConstituentStockInfo.do?isPagination=false&type=" + value + "&market=&etfClass=2&_=15257660" + str(random.randint(10000,99999))
                    logger.info(data_url)
                    # 获取的json数据
                    body = get_html(data_url, header1)
                    body_data = json.loads(body)
                    bodys = body_data["result"]
                except Exception as ex:
                    logger.exception(repr(ex))
                path = date_path + "/" + key + ".txt"
                # 写入文件
                with open(path, "w", encoding='utf-8') as file:
                    file.write('TradyingDay=' + str(result["tradingDay"]) + '|' + 'FundName=' + str(result["fundName"]) + '|'
                               + 'FundCompanyName=' + str(result["fundCompName"]) + '|' + 'FundInstrumentID1=' + str(result["fundId1"]) + '|'
                               + 'PreCashComponent=' + str(result["preCashComponent"]) + '|' + 'NavperCu=' + str(result["navpercu"]) + '|'
                               + 'Nav=' + str(result["nav"]) + '|' + 'EstimatedCashComponent=' + str(result["estimatedCashComponent"]) + '|'
                               + 'MaxCashRatio=' + str(result["maxCashRatio"]) + '| ' + 'CreationLimit=' + str(result["creationLimit"]) + '|'
                               + 'RedemptionLimit=' + str(result["redemptionLimit"]) + '|' + 'PublishIopvFlag=' + str(result["publishIopv"]) + '|'
                               + 'CreationRedemptionUnit=' + str(result["creationRedemptionUnit"]) + '|' + 'CreationRedemptionSwitch=' + str(result["creationRedemption"]) + '|' + '\n')
                    file.write('\n')
                    for b in bodys:
                        s = str(b["instrumentId"]).strip() + '|' + str(b["instrumentName"]).strip() + '|' + str(b["quantity"]).strip() + '|' + str(b["substituteFlag"]).strip() + '|'
                        if not b["premiumRate"]:
                            s = s + '|'
                        else:
                            s = s + str(b["premiumRate"]).strip() + '|'
                        if not b["cashAmount"]:
                            s = s + '|'
                        else:
                            s = s + str(b["cashAmount"]).strip() + '|'
                        file.write(s + '\n')
            logger.info(u"－－－开始休眠10秒－－－－")
            time.sleep(10)
        except Exception as e:
            logging.exception(u'抓取上交所页面数据定时任务出现异常')
            logger.exception(repr(e))
        finally:
            logger.info(u'抓取上交所页面数据定时任务结束')


# 抓取平安大华页面数据的任务
def job4(data):
        try:
            logger.info(u'平安大华定时任务开始')
            date_path = mk_dir2()
            for key, value in data.items():
                url = 'http://202.69.21.14:80/main/peanutFinance/yingPeanut/fundDetail/' + value + '.shtml'
                try:
                    data = get_html_gbk(url, header2)
                    html = BeautifulSoup(data, 'html.parser')
                    tables = html.find_all('table', class_='fljg_tab')
                    td = html.find_all('td', style="padding-left:20px; text-align:left;")
                except Exception as ex:
                    logger.exception(repr(ex))
                path = date_path + "/" + key + ".txt"
                with open(path, "w", encoding='utf-8') as file:
                    file.write('TradyingDay=' + td[0].text + '|' + 'FundName=' + td[1].text + '|' + 'FundCompanyName=' + td[
                        2].text + '|' + 'FundInstrumentID1=' + td[3].text + '|' + 'PreCashComponent=' + td[
                                   4].text + '|' + 'NavperCu=' + td[5].text + '|' + 'Nav=' + td[6].text + '|'
                               + 'EstimatedCashComponent=' + td[7].text + '|' + 'MaxCashRatio=' + td[
                                   8].text + '| ' + 'CreationLimit=' + td[9].text + '|' + 'RedemptionLimit=' + td[
                                   10].text + '|' + 'PublishIopvFlag=' + td[11].text + '|' + 'CreationRedemptionUnit=' + td[
                                   12].text + '|'
                               + 'CreationRedemptionSwitch=' + td[13].text + '|' + '\n')
                    logger.info(u'清单数据抓取完成')
                    s = ''
                    table = tables[6]
                    tr = table.find_all('tr')
                    for t in tr:
                        tds = t.find_all('td')
                        for td in tds:
                            s += td.text + '|'
                        file.write(s + '\n')
                        # 清空拼接的字符串
                        s = ''
                logger.info(u'多节点证券代码数据抓取完成')
            logger.info(u"－－－开始休眠10秒－－－－")
            time.sleep(10)
        except Exception as e:
            logging.exception(u'平安大华定时任务出现异常')
            logger.exception(repr(e))
        finally:
            logger.info(u'平安大华定时任务结束')


# 下载平安大华数据
def job5(data):
    try:
        logger.info(u"平安大华下载定时任务开始")
        # 创建日期文件夹
        date_path = mk_dir2()
        date = time.strftime("%Y-%m-%d")
        for key, value in data.items():
            etf_url = "http://202.69.21.14:80/servlet/EtfdownloadAction?tdate="+date+"&code="+value
            logger.info(etf_url)
            path = date_path + "/" + key + ".txt"
            try:
                urllib.request.urlretrieve(etf_url, path)
            except Exception as ex:
                logger.exception(repr(ex))
                count = 1
                while count <= 15:
                    try:
                        urllib.request.urlretrieve(etf_url, path)
                        break
                    except Exception as ep:
                        count += 1
                        logger.exception(repr(ep))
                if count > 15:
                    logger.exception(u"平安大华下载定时任务出现异常[" + key + "]下载失败")
            logger.info(u"－－－开始休眠10秒－－－－")
            time.sleep(10)
    except Exception as e:
        logger.exception(u"平安大华下载定时任务出现异常")
        logger.exception(repr(e))
    finally:
        logger.info(u"平安大华下载定时任务结束")

# 抓取平安大华页面数据(港股)的任务
def job6(data):
        try:
            logger.info(u'平安大华(港股)定时任务开始')
            date_path = mk_dir2()
            for key, value in data.items():
                url = 'http://202.69.21.14:80/main/peanutFinance/yingPeanut/fundDetail/' + value + '.shtml'
                try:
                    data = get_html_gbk(url, header2)
                    html = BeautifulSoup(data, 'html.parser')
                    tables = html.find_all('table', class_='fljg_tab')
                    td = html.find_all('td', style="padding-left:20px; text-align:left;")
                except Exception as ex:
                    logger.exception(repr(ex))
                path = date_path + "/" + key + ".txt"
                with open(path, "w", encoding='utf-8') as file:
                    file.write('TradyingDay=' + td[0].text + '|' + 'Symbol=' + td[1].text + '|' + 'FundManagementCompany='
                               + td[2].text + '|' + 'SecurityID=' + td[3].text + '|' + 'UnderlyingSecurityID='
                               + td[4].text + '|' + 'CashComponent=' + td[5].text + '|' + 'NAVperCU='
                               + td[6].text + '|' + 'NAV=' + td[7].text + '|' + 'EstimateCashComponent='
                               + td[8].text + '| ' + 'MaxCashRatio=' + td[9].text + '|' + 'CreationLimit='
                               + td[10].text + '|' + 'RedemptionLimit=' + td[11].text + '|' + 'Publish='
                               + td[12].text + '|' + 'CreationRedemptionUnit=' + td[13].text + '|' + 'CreationAndRedemption='
                               + td[14].text + '|' + 'DividendPerCU=' + td[15].text + '|' + 'CreationLimitPerUser='
                               + td[16].text + '|' + 'RedemptionLimitPerUser=' + td[17].text + '|' + 'NetCreationLimitPerUser='
                               + td[18].text + '|' + 'NetRedemptionLimitPerUser=' + td[19].text + '|'
                               + '\n')
                    logger.info(u'清单数据抓取完成')
                    s = ''
                    table = tables[5]
                    tr = table.find_all('tr')
                    for t in tr:
                        tds = t.find_all('td')
                        for td in tds:
                            s += td.text + '|'
                        file.write(s + '\n')
                        # 清空拼接的字符串
                        s = ''
                logger.info(u'多节点证券代码数据抓取完成')
            logger.info(u"－－－开始休眠10秒－－－－")
            time.sleep(10)
        except Exception as e:
            logging.exception(u'平安大华(港股)定时任务出现异常')
            logger.exception(repr(e))
        finally:
            logger.info(u'平安大华(港股)定时任务结束')

# 上交所下载
def job1_task():
    logger.info(u'进入上交所定时任务多线程')
    threading.Thread(target=job1(fund_code1, data_dict1)).start()
    threading.Thread(target=job1(fund_code2, data_dict2)).start()


# 上交所下载
def job2_task():
    logger.info(u'进入上交所下载定时任务')
    threading.Thread(target=job2(data_down)).start()


# 上交所页面所下载
def job3_task():
    logger.info(u'进入上交所页面下载定时任务')
    threading.Thread(target=job3(data_down1)).start()


# 平安大华页面下载
def job4_task():
    logger.info(u'进入平安大华页面下载定时任务多线程')
    threading.Thread(target=job4(data_down2)).start()


# 平安大华下载
def job5_task():
    logger.info(u'进入平安大华下载定时任务')
    threading.Thread(target=job5(data_down3)).start()

# 平安大华页面下载(港股)
def job6_task():
    logger.info(u'进入平安大华页面下载定时任务多线程(港股)')
    threading.Thread(target=job6(data_down4)).start()

# 时间配置
trigger_time = "8:35"
schedule.every().day.at(trigger_time).do(job1_task)
schedule.every().day.at(trigger_time).do(job2_task)
schedule.every().day.at(trigger_time).do(job3_task)
schedule.every().day.at(trigger_time).do(job4_task)
schedule.every().day.at(trigger_time).do(job5_task)
schedule.every().day.at(trigger_time).do(job6_task)

if __name__ == '__main__':
    logger.info(u'python定时任务启动成功，将在[%s]执行。' % trigger_time)
    while True:
        schedule.run_pending()
        time.sleep(5)
