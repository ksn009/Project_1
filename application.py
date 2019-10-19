from flask import Flask, request, render_template
import requests
import time
import mysql.connector
import json
import csv
from datetime import datetime
from io import StringIO
from werkzeug.wrappers import Response


#sql = "INSERT INTO scrap_data (api,pt,link) VALUES (%s, %s, %s)"
#mycursor.executemany(sql, v)
#mydb.commit()
        
mydb = mysql.connector.connect(
  host="localhost",
  #user="admin",
  #passwd="vlvdW0IWRkTDfkvpk3m6",
  #database="flaskdb"
  user="root",
  passwd="",
  database="scrape_parsehub"
)
mycursor = mydb.cursor()
v=[]
api_list=['t5QpzJ5WqUoE','tqfTT-tZebt0','t8yq90SEaHk9','tvOFW19xBdBz','tvct8117-qjo','tKhMgLLqenM3','ttTxMj1hYnoZ','t0F5BD-Y96tq',
     'tH91mAar-nHP','twhXaTsgn49Z','tpwc8cPfTSLv','tqxv9bsXmEnM','ttkS859s2wku','ty4AjFWxmLXD','t6svuaX8kXSt','tvmzBHODR8-m',
     'tNLjeYj6XVLE','tTgnG2Be7wih','tv47dCOmiavk','tavQyKTnj42O','tiAgn1aY2jrz','tdMK7tV6_qZq','tG1eo8sNGmYP','tf7o_zyLPoYb',
     'tziC4p8x1BJE','tfShBYRKUpEY','ty_o6U-G6CRT','tb2z_mvTyfEH','t-f4O6OUqNS7','tY4VW1-gceYR']
    
pt_list =['tBe8vTtLNtsz','tc7e5MbL4C0T','trnH1KDqO8X_','tQXeOTewtAxn','th1oxdz-USyE','t-2RTFDb3akO','t7cv66Oewnpx','tsyPyJgX40Ej',
     'tDELs5G_L7Yz','tjdMRk9c6uX0','tTnduJ1HC1sF','tYuWGMYmpnGe','tHJxMUX_i8XS','tWvd_oxeTz2t','tVqj57CtGzZh','t5MuG7hFE5GT',
     't80z50WvMRsa','t2vv-7SKe6pb','tdX4cf6bvDka','tVSSe7Q2P66Z','txwapiv7WqRz','tBL8LL3Amj5H','tenzhNpXLCqm','tTGfg7k0FJT8',
     'toTTR3Zswa4g','tkwQ4hQgYtkx','tE_JG_7OfMBJ','t8h6eCJUAzVZ','t1t3pRyJpsVE','tCi8FEq2TVYq']

api=[]
pt=[]
    
now = datetime.now()
now1="project1_"+str(now)+".csv"
app = Flask(__name__)

@app.route('/')
def my_form():
    return render_template('index.html')


@app.route('/run', methods=['POST'])
def run():
    return render_template('run.html')

@app.route('/get', methods=['POST'])
def get():
    drop=[]
    mycursor = mydb.cursor()
    mycursor.execute("SELECT DISTINCT run_name FROM input_data")
    myresult = mycursor.fetchall()
    for t in myresult:
        drop.append(t[0])
    return render_template('get.html',runt_drop=drop)

@app.route('/run_data', methods=['POST'])
def run_data():
    mycursor.execute("CREATE TABLE IF NOT EXISTS input_data (id INT AUTO_INCREMENT PRIMARY KEY, api VARCHAR(25), pt VARCHAR(25), link VARCHAR(255), runt VARCHAR(25), run_name VARCHAR(25))")

    for n in range(0,len(api_list)):
        params = {
                "api_key": api_list[n],
                "offset": "0",
                "limit": "20",
                "include_options": "1"
                }
        r = requests.get('https://www.parsehub.com/api/v2/projects', params=params)
        d=json.loads(r.text)
        for i in range(0,int(d['total_projects'])):
            if (d['projects'][i]['last_run'] == None) or (d['projects'][i]['last_run']['status'] == 'complete'):
                ready = True
                #elif (d['projects'][i]['last_run']['status'] == 'complete'):
                #    ready = True
            else:
                ready = False
                break
    
        if ready == True:
            api.append(api_list[n])
            pt.append(pt_list[n])

    link= request.form['link']
    link1=link.split()
    link_len=len(link1)

    r_name= request.form['r_name']
    if r_name == "":
        return "Error : Please Enter Valid Run Name"
    
    max_link = len(api)*199
    
    un_processing_link_numb=0
    
    processing_link=[]
    un_processing_link=[]
    links199=[]

    h=0
    
    run_tok=""
    res=""
    link_temp=""
    #j='\\",\\"'
    j="\",\""
    
    
    if link_len != 0:
    
        if link_len < 199:
            api_sel= 1
            api_r = 0
            count_start=0
            count_max=link_len
            count_max_temp=link_len
            
            count_start_sql=0
            count_max_sql=link_len
            
        elif link_len > max_link:
            api_sel=len(api)
            api_r=0
            count_start=0
            count_max=max_link
            
            count_start_sql=0
            count_max_sql=max_link
            count_max_temp=199
            
        else:
            api_sel = int(link_len/199)
            if link_len%199 != 0:
                api_r = 1
            else:
                api_r = 0
            count_start = 0
            count_max=link_len
            count_max_temp=199
            
            count_start_sql = 0
            count_max_sql=199
            
        
        if link_len > max_link:
            for n in range(0,max_link):
                processing_link.append(link1[n])
            un_processing_link_numb=link_len-max_link
            for n in range((link_len-un_processing_link_numb),link_len):
                un_processing_link.append(link1[n])
        else:
            for n in range(0,link_len):
                processing_link.append(link1[n])
        
        if link_len < max_link:
            for ii in range(0,api_sel):
                link_temp199=[]
                for i in range(count_start,count_max_temp):
                    link_temp199.append(processing_link[i])
                count_start=i+1
                if (link_len-count_start) >= 199:
                    count_max_temp = count_max_temp + 199
                else:
                    count_max_temp=link_len
                link_temp=j.join(link_temp199)
                links199.append("{\"url\":[\""+link_temp+"\"]}")
                params = {
                        "api_key": api[ii],
                        "start_url": "https://www.amazon.in/","start_template": "main_template",
                        "start_value_override": links199[ii],
                        "send_email": "1"
                        }
                r = requests.post("https://www.parsehub.com/api/v2/projects/"+pt[ii]+"/run", data=params)
                res = res +'<br>'+ r.text
                y=json.loads(r.text)
                if y['run_token'] != '':
                    for s in range(count_start_sql,count_max_sql):
                        sql = "INSERT INTO input_data (api,pt,link,runt,run_name) VALUES (%s, %s, %s, %s, %s)"
                        v=(api[ii],pt[ii],processing_link[s],y['run_token'],r_name)
                        mycursor.execute(sql, v)
                        mydb.commit()
                    count_start_sql=s+1
                    if (link_len-count_start_sql) > 199:
                        count_max_sql=count_max_sql+199
                    else:
                        count_max_sql=link_len
                
                    run_tok=run_tok+'<br>'+y['run_token']
                else:
                    return "Error in Running Project"+'<br><br>'+res
                h=ii+1
            link_temp199=[]
            v=[]
            if api_r != 0:
                for i in range(count_start,count_max_temp):
                    link_temp199.append(processing_link[i])
                link_temp=j.join(link_temp199)
                links199.append("{\"url\":[\""+link_temp+"\"]}")
                params = {
                    "api_key": api[h],
                    "start_url": "https://www.amazon.in/","start_template": "main_template",
                    "start_value_override": links199[h],
                    "send_email": "1"
                    }
                r = requests.post("https://www.parsehub.com/api/v2/projects/"+pt[h]+"/run", data=params)
                res = res +'<br>'+ r.text
                y=json.loads(r.text)
                if y['run_token'] != '':
                    for s in range(count_start_sql,count_max_sql):
                        sql = "INSERT INTO input_data (api,pt,link,runt,run_name) VALUES (%s, %s, %s, %s, %s)"
                        v=(api[h],pt[h],processing_link[s],y['run_token'],r_name)
                        mycursor.execute(sql, v)
                        mydb.commit()                    
                    
                    run_tok=run_tok+'<br>'+y['run_token']
                else:
                    return "Error in Running Project"+'<br><br>'+res
            res="Successfully Completed "+str(h)+'<br>'+" Projects Run Name: "+r_name+'<br><br>'+ 'Run Token For Projects:'+'<br>'+run_tok
            return res
        else:
            for ii in range(0,31):
                link_temp199=[]
                for i in range(count_start,count_max):
                    link_temp199.append(processing_link[i])
                count_start=i+1
                if (link_len-count_start) > 199:
                    count_max_temp = count_max_temp+199
                else:
                    count_max_temp=link_len
                link_temp=j.join(link_temp199)
                links199.append("{\"url\":[\""+link_temp+"\"]}")
                params = {
                        "api_key": api[ii],
                        "start_url": "https://www.amazon.in/","start_template": "main_template",
                        "start_value_override": links199[ii],
                        "send_email": "1"
                        }
                r = requests.post("https://www.parsehub.com/api/v2/projects/"+pt[ii]+"/run", data=params)
                res = res +'<br>'+ r.text
                y=json.loads(r.text)
                if y['run_token'] != '':
                    for s in range(count_start_sql,count_max_sql):
                        sql = "INSERT INTO input_data (api,pt,link,runt,run_name) VALUES (%s, %s, %s, %s, %s)"
                        v=(api[ii],pt[ii],processing_link[s],y['run_token'],r_name)
                        mycursor.execute(sql, v)
                        mydb.commit()
                    count_start_sql=s+1
                    if (link_len-count_start_sql) > 199:
                        count_max_sql=count_max_sql+199
                    else:
                        count_max_sql=link_len
                    run_tok=run_tok+'<br>'+y['run_token']
                else:
                    return "Error in Running Project"+'<br><br>'+res
                time.sleep(5)
            res="Successfully Completed "+str('30')+" Projects Run Name: "+r_name+'<br><br>'+'Run Token For Projects:'+'<br>'+run_tok+'<br><br>'+"Un Processed Links Due to Over Load <br>" 
            for nn in range(0,un_processing_link_numb):
                res = res + '<br>' + un_processing_link[nn]		
            return res
            
    else:
        return "Please Enter Vaild Data"

@app.route('/get_data', methods=['POST'])
def get_data():
    mycursor = mydb.cursor()
    mycursor.execute("CREATE TABLE IF NOT EXISTS scrap_data (id INT AUTO_INCREMENT PRIMARY KEY, product_name VARCHAR(800), product_url VARCHAR(800), runt VARCHAR(25), ip_link VARCHAR(255), run_name VARCHAR(25))")

    v=[]
    p_url=[]
    p_name=[]
    ip_link=[]
    ip_url=[]
    run_token=[]
    run_token_sql=[]
    api1=[]
    
    r_name = request.form['runt_drop']

    sql = "SELECT DISTINCT runt FROM input_data WHERE run_name = %s"
    adr = (r_name,)
    mycursor.execute(sql, adr)
    ip_l = mycursor.fetchall()
    for t in ip_l:
        run_token.append(t[0])

    sql = "SELECT DISTINCT link FROM input_data WHERE run_name = %s"
    adr = (r_name,)
    mycursor.execute(sql, adr)
    ip_l = mycursor.fetchall()
    for t in ip_l:
        ip_url.append(t[0])
        
    sql = "SELECT DISTINCT api FROM input_data WHERE run_name = %s"
    adr = (r_name,)
    mycursor.execute(sql, adr)
    api = mycursor.fetchall()
    for t in api:
        api1.append(t[0])

    sql = "SELECT DISTINCT runt FROM scrap_data WHERE run_name LIKE '%"+r_name+"%'"
    adr = (r_name,)
    mycursor.execute(sql)
    store_var = mycursor.fetchall()
    
    for t in range(0,len(api1)):
        params = {
                "api_key": api1[t]
                }
        r = requests.get('https://www.parsehub.com/api/v2/runs/'+run_token[t], params=params)
        y=json.loads(r.text)
        if y['status'] == 'complete':
            continue
        else:
            return " PLEASE WAIT.... Data Not Ready "

    if len(store_var) == 0:
        for z in range(0,len(api1)):
            params = {"api_key": api1[z],"format": "json"}
            r = requests.get('https://www.parsehub.com/api/v2/runs/'+run_token[z]+'/data', params=params)
            #r = requests.get('https://www.parsehub.com/api/v2/projects/'+pt1[ii]+'/last_ready_run/data', params=params)
            y = json.loads(r.text)
            for n in range (0,len(y['details'])):
                if 'selection1' in (y['details'][n]):
                    for nn in range (0,len(y['details'][n]['selection1'])):
                        if ("name" in y['details'][n]['selection1'][nn]) and ("url" in y['details'][n]['selection1'][nn]):
                            p_name.append(y['details'][n]['selection1'][nn]['name'])
                            p_url.append(y['details'][n]['selection1'][nn]['url'])
                        #else:
                        #    p_name.append("")
                    #for nn in range (0,len(y['details'][n]['selection1'])):
                    #    if "url" in y['details'][n]['selection1'][nn]:
                            #p_url.append(y['details'][n]['selection1'][nn]['url'])
                     #   else:
                     #       p_url.append("")
                    for nn in range (0,len(y['details'][n]['selection1'])):
                        ip_link.append(ip_url[n])
                    for nn in range (0,len(y['details'][n]['selection1'])):
                        run_token_sql.append(run_token[z])
                else:
                    continue
                    #return("ERROR PLEASE CHECK INPUT!!")

        p_len=len(p_name)        
        for i in range(0,p_len):
            v.append((p_name[i],p_url[i],run_token_sql[i],ip_link[i],r_name))
            
        def generate():
            data = StringIO()
            w = csv.writer(data)

            # write header
            w.writerow(('Product_name', 'Product_URL','Input Link'))
            yield data.getvalue()
            data.seek(0)
            data.truncate(0)
            # write each log item
            for item in v:
                w.writerow((
                    item[0],
                    item[1],#.isoformat()  # format datetime as string
                    item[3]
                    ))
                yield data.getvalue()
                data.seek(0)
                data.truncate(0)
        response = Response(generate(), mimetype='text/csv')
        
        sql = "INSERT INTO scrap_data (product_name,product_url,runt,ip_link,run_name) VALUES (%s, %s, %s, %s, %s)"
        mycursor.executemany(sql, v)
        mydb.commit()
        
        response.headers.set("Content-Disposition", "attachment", filename=now1)
        return response
    else:
        sql = "SELECT product_name, product_url, ip_link FROM scrap_data WHERE run_name LIKE '%"+r_name+"%'"
        adr = (r_name,)
        mycursor.execute(sql)
        store_var = mycursor.fetchall()
        for t in store_var:
            v.append((t[0],t[1],t[2]))
            
        def generate():
            data = StringIO()
            w = csv.writer(data)
            # write header
            w.writerow(('Product_name', 'Product_URL','Input Link'))
            yield data.getvalue()
            data.seek(0)
            data.truncate(0)
            # write each log item
            for item in v:
                w.writerow((
                    item[0],
                    item[1],#.isoformat()  # format datetime as string
                    item[2]
                    ))
                yield data.getvalue()
                data.seek(0)
                data.truncate(0)

        response = Response(generate(), mimetype='text/csv')
        response.headers.set("Content-Disposition", "attachment", filename=now1)
        return response
        
if __name__ == '__main__':
    app.run()    
