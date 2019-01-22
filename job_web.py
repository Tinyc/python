from bottle import Bottle, run, request, template
import job_task
import os


def is_path(path):
    if not path:
        return False
    elif not os.path.isdir(path):
        return False
    else:
        return True


app_job = Bottle()


@app_job.route('/', method='GET')
def home():
    return '''<form action="/start" method="post">
            <span style="color:blue">----------------上交所、平安大华官网数据爬取----------------</span><br><br>
            下载路径:<input name="path" type="text" style="width:340px" value="S:/puf-etfms" /><br><br>
            设置时间:<input name="hour" type="text" value="8" />时:<input name="minute" type="text" value="36" />分
            <input value="启动定时任务" type="submit"/>
            </form>'''


@app_job.route('/start', method='POST')
def start():
    try:
        path = request.forms.get('path')
        hour_str = request.forms.get('hour')
        minute_str = request.forms.get('minute')
        hour = int(hour_str)
        minute = int(minute_str)
        if not is_path(path):
            return template('''<form action="/start" method="post">
                            <span style="color:blue">----------------上交所、平安大华官网数据爬取----------------</span><br><br>
                            下载路径:<input name="path" type="text" style="width:340px" value="S:/puf-etfms" /><span style="color:red">路径为空或者不存在！</span><br><br>
                            设置时间:<input name="hour" type="text" value="8"/>时:
                            <input name="minute" type="text" value="36" />分
                            <input value="启动定时任务" type="submit"/>
                            </form>''')
        elif 0 > hour or hour >= 24 or 0 > minute or minute >= 60:
            return template('''<form action="/start" method="post">
                            <span style="color:blue">----------------上交所、平安大华官网数据爬取----------------</span><br><br>
                            下载路径:<input name="path" type="text" style="width:340px" value="S:/puf-etfms"/><br><br>
                            设置时间:<input name="hour" type="text" value="8"/>时:
                            <input name="minute" type="text" value="36" />分
                            <input value="启动定时任务" type="submit"/>
                            <span style="color:red">时间格式错误，请重新设置！</span>
                            </form>''')
        else:
            job_task.run_task(path, hour, minute)
            return template(
                '<span style="color:blue">定时任务启动成功，将在每天:{{hour}}:{{minute}}执行,数据存放在:{{path}}。</span>',
                hour=hour, minute=minute, path=path)

    except Exception as e:
        print(repr(e))
        return template('''<form action="/start" method="post">
                        <span style="color:blue">----------------上交所、平安大华官网数据爬取----------------</span><br><br>
                        下载路径:<input name="path" type="text" style="width:340px" value="S:/puf-etfms"/><br><br>
                        设置时间:<input name="hour" type="text" value="8"/>时:
                        <input name="minute" type="text" value="36" />分
                        <input value="启动定时任务" type="submit"/>
                        <span style="color:red">配置异常！</span>
                        </form>''')


run(app_job, host='127.0.0.1', port='8010')
