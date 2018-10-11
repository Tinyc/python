from bottle import Bottle,run,request,template
import pcf_task

app=Bottle()
@app.route('/', method='GET')
def home():
    return '''<form action="/start" method="post">
                基金代码:<input name="codes" type="text" style="width:340px"/>(多基金用英文逗号分隔)<br><br>
                下载路径:<input name="path" type="text" style="width:340px" value="S:/puf-etfms" /><br><br>
                设置时间:<input name="hour" type="text" value="8" />时:<input name="minute" type="text" value="35" />分
                <input value="启动定时任务" type="submit"/>
            </form>'''

@app.route('/start', method='POST')
def start():
    try:
        codes = request.forms.get('codes')
        path = request.forms.get('path')
        hour_str = request.forms.get('hour')
        minute_str = request.forms.get('minute')
        hour = int(hour_str)
        minute = int(minute_str)
        if not codes:
            return template('''<form action="/start" method="post">
                                   基金代码:<input name="codes" type="text" style="width:340px" />(多基金用英文逗号分隔)<span style="color:red">基金代码不能为空！</span><br><br>
                                   下载路径:<input name="path" type="text" style="width:340px" value="S:/puf-etfms" /><br><br>
                                   设置时间:<input name="hour" type="text" value="8"/>时:
                                   <input name="minute" type="text" value="35" />分
                                   <input value="启动定时任务" type="submit"/>
                               </form>''')
        elif not path:
            return template('''<form action="/start" method="post">
                                               基金代码:<input name="codes" type="text" style="width:340px" />(多基金用英文逗号分隔)<br><br>
                                               下载路径:<input name="path" type="text" style="width:340px" value="S:/puf-etfms" /><span style="color:red">路径不能为空！</span><br><br>
                                               设置时间:<input name="hour" type="text" value="8"/>时:
                                               <input name="minute" type="text" value="35" />分
                                               <input value="启动定时任务" type="submit"/>
                                           </form>''')
        elif 0 > hour or hour >= 24 or 0 > minute or minute >= 60:
            return template('''<form action="/start" method="post">
                                               基金代码:<input name="codes" type="text" style="width:340px" />(多基金用英文逗号分隔)<br><br>
                                               下载路径:<input name="path" type="text" style="width:340px" value="S:/puf-etfms"/><br><br>
                                               设置时间:<input name="hour" type="text" value="8"/>时:
                                               <input name="minute" type="text" value="35" />分
                                               <input value="启动定时任务" type="submit"/>
                                               <span style="color:red">时间格式错误，请重新设置！</span>
                                           </form>''')
        else:
            pcf_task.run_task(codes, path, hour, minute)
            return template(
            '<span style="color:blue">定时任务启动成功，将在每天:{{hour}}:{{minute}}爬取:{{codes}}基金的数据,存放在:{{path}}。</span>',
            hour=hour, minute=minute, codes=codes, path=path)

    except Exception as e:
        print(repr(e))
        return template('''<form action="/start" method="post">
                       基金代码:<input name="codes" type="text" style="width:340px" />(多基金用英文逗号分隔)<br><br>
                       下载路径:<input name="path" type="text" style="width:340px" value="S:/puf-etfms"/><br><br>
                       设置时间:<input name="hour" type="text" value="8"/>时:
                       <input name="minute" type="text" value="35" />分
                       <input value="启动定时任务" type="submit"/>
                       <span style="color:red">配置异常！</span>
                   </form>''')
run(app, host='127.0.0.1', port='8020')