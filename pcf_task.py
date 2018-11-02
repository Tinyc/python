import urllib.request
import time
import os
import logging
import sys
import random
import threading
from logging.handlers import TimedRotatingFileHandler
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

# 当前文件的路径
task_path = os.getcwd()
# 日志
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger_path = task_path + '/task_web.log'
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler = TimedRotatingFileHandler(filename=logger_path, when="D", interval=1, backupCount=3, encoding='utf-8')
file_handler.suffix = '%Y-%m-%d.log'
file_handler.setFormatter(formatter)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.formatter = formatter
logger.addHandler(file_handler)
logger.addHandler(console_handler)

header = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
    'Accept-Language': 'zh-CN,zh;q=0.9',
    'Connection': 'keep-alive',
    'Host': 'reportdocs.static.szse.cn',
    'Referer': 'http://www.szse.cn/disclosure/fund/currency/index.html',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36'
}


# 单例
class Borg(object):
    @classmethod
    def get_scheduler(cls):
        try:
            cls_scheduler = cls.scheduler
        except AttributeError as e:
            cls.scheduler = BackgroundScheduler()
            logger.exception(repr(e))
            return cls.scheduler
        else:
            return cls_scheduler


# 需用同步锁，防止两个线程同时创建文件夹
mutex = threading.Lock()


def mk_dir(file_path):
    date_path = ''
    mutex.acquire()
    logger.info('开始创建文件夹,并对文件夹加锁......')
    try:
        if not os.path.isdir(file_path):
            logger.info('配置路径不存在，创建文件夹失败，路径%s不存在' % file_path)
        else:
            date = time.strftime('%Y%m%d')
            date_path = file_path + '/' + 'download' + '/' + date
            if not os.path.exists(date_path):
                os.makedirs(date_path)
                logger.info('文件夹%s创建成功' % date_path)
            else:
                logger.info('文件夹%s已经存在，无需创建' % date_path)
    except Exception as e:
        logger.exception('创建文件夹出现异常')
        logger.exception(repr(e))
    finally:
        logger.info('创建文件夹结束,开始解锁......')
        mutex.release()
    return date_path


# 获取数据
def write_html(url, path):
    req = urllib.request.Request(url, headers=header)
    html = urllib.request.urlopen(req).read()
    txt = html.decode('gbk', errors='replace')
    with open(path, 'w') as file:
        file.write(txt)


# 下载深圳证券交易所的定时任务
def job1(arr, file_path):
    try:
        logger.info('开始深圳证券交易所下载的定时任务')
        date_path = mk_dir(file_path)
        date = time.strftime('%Y%m%d')
        for each in arr:
            if each:
                etf_url = 'http://reportdocs.static.szse.cn/files/text/ETFDown/pcf_' + each + '_' + date + '.xml'
                path = date_path + '/pcf_' + each + '_' + date + '.xml'
                logger.info('下载路径:[' + etf_url + '],生成路径:[' + path + ']')
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
                        logger.exception('深圳证券交易所下载定时任务出现异常[' + 'pcf_' + each + '_' + date + '.xml' + ']下载失败')
                logger.info('－－－开始休眠2秒－－－')
                time.sleep(2)
    except Exception as e:
        logger.exception('深圳证券交易所下载定时任务出现异常')
        logger.exception(repr(e))
    finally:
        logger.info('深圳证券交易所下载定时任务结束')


# 爬取深圳证券交易所的定时任务
def job2(arr, file_path):
    try:
        logger.info('开始深圳证券交易所爬取的定时任务')
        date_path = mk_dir(file_path)
        date = time.strftime('%Y%m%d')
        for each in arr:
            if each:
                etf_url = 'http://reportdocs.static.szse.cn/files/text/etf/ETF' + each + date + '.txt?random=' + str(
                    random.random())
                path = date_path + '/ETF' + each + date + '.txt'
                logger.info('爬取路径:[' + etf_url + '],生成路径:[' + path + ']')
                try:
                    write_html(etf_url, path)
                except Exception as ex:
                    logger.exception(repr(ex))
                    count = 1
                    while count <= 15:
                        try:
                            write_html(etf_url, path)
                            break
                        except Exception as ep:
                            count += 1
                            logger.exception(repr(ep))
                    if count > 15:
                        logger.exception('深圳证券交易所爬取定时任务出现异常[' + 'ETF' + each + date + '.txt' + ']爬取失败')
                logger.info('－－－开始休眠2秒－－－')
                time.sleep(2)
    except Exception as e:
        logger.exception('深圳证券交易所爬取定时任务出现异常')
        logger.exception(repr(e))
    finally:
        logger.info('深圳证券交易所爬取定时任务结束')


def run_task(codes, path, hour, minute):
    scheduler = Borg.get_scheduler()
    logger.info(scheduler)
    try:
        file_path = path.replace('\\', '/').replace('\\', '/')
        arr = codes.split(',')
        trigger = CronTrigger(day_of_week='0-6', hour=hour, minute=minute, second='0')

        flag = False
        if scheduler.get_job('task_one'):
            scheduler.reschedule_job(job_id='task_one', trigger=trigger, args=[arr, file_path], misfire_grace_time=120)
        else:
            scheduler.add_job(func=job1, trigger=trigger, args=[arr, file_path], misfire_grace_time=120, id='task_one')
            flag = True

        if scheduler.get_job('task_two'):
            scheduler.reschedule_job(job_id='task_two', trigger=trigger, args=[arr, file_path], misfire_grace_time=120)
        else:
            scheduler.add_job(func=job2, trigger=trigger, args=[arr, file_path], misfire_grace_time=120, id='task_two')
            flag = True

        if flag:
            scheduler.start()

        logger.info('python定时任务启动成功，将在[%s]:[%s]执行。可访问 http://127.0.0.1:8020/ 进行修改时间' % (hour, minute))
        logger.info('爬取的基金代码为:[%s]' % arr)
        logger.info('保存路径为:[%s]' % path)
    except Exception as e:
        logger.exception(repr(e))
        scheduler.shutdown()
    finally:
        return True
