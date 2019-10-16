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
api=['tvct8117-qjo','t5QpzJ5WqUoE','tBe8vTtLNtsz','tc7e5MbL4C0T','trnH1KDqO8X_','tQXeOTewtAxn','th1oxdz-USyE','t-2RTFDb3akO','t7cv66Oewnpx','tsyPyJgX40Ej','tDELs5G_L7Yz','tjdMRk9c6uX0','tTnduJ1HC1sF',
    'tYuWGMYmpnGe','tHJxMUX_i8XS','tWvd_oxeTz2t','tVqj57CtGzZh','t5MuG7hFE5GT','t80z50WvMRsa','t2vv-7SKe6pb','tdX4cf6bvDka','tVSSe7Q2P66Z','txwapiv7WqRz','tBL8LL3Amj5H',
    'tenzhNpXLCqm','tTGfg7k0FJT8','toTTR3Zswa4g','tkwQ4hQgYtkx','tE_JG_7OfMBJ','t8h6eCJUAzVZ','t1t3pRyJpsVE','tCi8FEq2TVYq']
    
pt=['th1oxdz-USyE','tBe8vTtLNtsz','tBuMgiaeTShh','tLsP7XuRWiZv','togpopbnfKWn','t-CwbbnVZiud','tV6eTx5vUtQF','t8cTLOMwRBet','tjaf2jTFW2gk','txrjxwRzTV0n','tm9uY98Ahy8T','tXUzMWwz9s0t','tTELu-KmgHdx',
    'tqwihzXTi951','tCd0XUwx69Vd','tO_59rUkKmoO','thOY9jvdUS7i','tGywyPjYZkxf','tjKuJY7EiNqW','tcUpcahGXOhd','t_DRpTopOvJS','turymWa5qcgK','tTEg_zTps9HX','tUaaHD9dcvo6',
    't4d56wdi0Ynn','tkxNrgV-JYB0','tXACMk40cV27','tF6LbUX9EkOQ','tnrAjd5qZwQY','t190wTsTrWqq','tj0cJDAAtGTK','tg8R8bUcZrqF']
    
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
    mycursor.execute("SELECT DISTINCT runt FROM input_data")
    myresult = mycursor.fetchall()
    for t in myresult:
        drop.append(t[0])
    return render_template('get.html',runt_drop=drop)

@app.route('/run_data', methods=['POST'])
def run_data():
    link= request.form['link']
    link1=link.split()
    link_len=len(link1)

    max_link = 5970
    un_processing_link_numb=0
    processing_link=[]
    un_processing_link=[]
    links199=[]
    h=0
    
    mycursor.execute("CREATE TABLE IF NOT EXISTS input_data (id INT AUTO_INCREMENT PRIMARY KEY, api VARCHAR(25), pt VARCHAR(25), link VARCHAR(255), runt VARCHAR(25))")

    run_tok=""
    res=""
    link_temp=""
    #j='\\",\\"'
    j="\",\""

        
    temp_count = 0
    temp_count_sql = 0
    #temp_count_1=""
                                                      # MAXIMUM LINKS 
    if link_len != 0:
        #return "Please Enter Vaild Data"
    
        if link_len > max_link:
            ip_link=199
            #return str(ip_link)+'if'
        elif link_len<199:
            ip_link= 1
            ip_link_remaining = 0
        else:
            ip_link = int(link_len/199)
            if ip_link%199 != 0:
                ip_link_remaining = (link_len-(ip_link*199))
        
        temp_count_max = int(ip_link)    
        if link_len > 199:
            temp_count_max_1 = 199
            temp_count_max_1_sql = 199
        else:
            temp_count_max_1 = link_len
            temp_count_max_1_sql = link_len
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
            for ii in range(0,ip_link):
                link_temp199=[]
                for i in range(temp_count,temp_count_max_1):
                    link_temp199.append(processing_link[i])
                temp_count=i+1
                temp_count_max_1=temp_count_max_1+temp_count_max
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
                    for s in range(temp_count_sql,temp_count_max_1_sql):
                        sql = "INSERT INTO input_data (api,pt,link,runt) VALUES (%s, %s, %s, %s)"
                        v=(api[ii],pt[ii],processing_link[s],y['run_token'])
                        mycursor.execute(sql, v)
                        mydb.commit()
                    temp_count_sql=s+1
                    temp_count_max_1_sql=temp_count_max_1_sql+temp_count_max
                    run_tok=run_tok+'<br>'+y['run_token']
                else:
                    return "Error in Running Project"+'<br><br>'+res
                time.sleep(1)
                h=ii+1
            link_temp199=[]
            v=[]
            if ip_link_remaining != 0:
                temp_count_max_1=temp_count_max_1+ip_link_remaining
                for i in range(temp_count,temp_count_max_1):
                    link_temp199.append(processing_link[i])
                link_temp=j.join(link_temp199)
                links199.append("{\"url\":[\""+link_temp+"\"]}")
                params = {
                    "api_key": api[ii+1],
                    "start_url": "https://www.amazon.in/","start_template": "main_template",
                    "start_value_override": links199[1],
                    "send_email": "1"
                    }
                r = requests.post("https://www.parsehub.com/api/v2/projects/"+pt[ii]+"/run", data=params)
                res = res +'<br>'+ r.text
                y=json.loads(r.text)
                if y['run_token'] != '':
                    temp_count_max_1_sql=temp_count_max_1_sql+ip_link_remaining
                    for s in range(temp_count_sql,temp_count_max_1_sql):
                        sql = "INSERT INTO input_data (api,pt,link,runt) VALUES (%s, %s, %s, %s)"
                        v=(api[ii],pt[ii],processing_link[s],y['run_token'])
                        mycursor.execute(sql, v)
                        mydb.commit()
                    temp_count_sql=s+1
                    temp_count_max_1_sql=temp_count_max_1_sql+temp_count_max
                    run_tok=run_tok+'<br>'+y['run_token']
                else:
                    return "Error in Running Project"+'<br><br>'+res
                
                time.sleep(1)
                
            res="Successfully Completed "+str(h)+" Projects"+'<br><br>'+ 'Run Token For Projects:'+'<br>'+run_tok
            return res
        else:
            for ii in range(0,31):
                link_temp199=[]
                for i in range(temp_count,temp_count_max_1):
                    link_temp199.append(processing_link[i])
                temp_count=i+1
                temp_count_max_1=temp_count_max_1+temp_count_max
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
                    temp_count_max_1_sql=temp_count_max_1_sql+ip_link_remaining
                    for s in range(temp_count_sql,temp_count_max_1_sql):
                        sql = "INSERT INTO input_data (api,pt,link,runt) VALUES (%s, %s, %s, %s)"
                        v=(api[ii],pt[ii],processing_link[s],y['run_token'])
                        mycursor.execute(sql, v)
                        mydb.commit()
                    temp_count_sql=s+1
                    temp_count_max_1_sql=temp_count_max_1_sql+temp_count_max
                    run_tok=run_tok+'<br>'+y['run_token']
                else:
                    return "Error in Running Project"+'<br><br>'+res
                time.sleep(5)
            res="Successfully Completed "+str('30')+" Projects"+'<br><br>'+'Run Token For Projects:'+'<br>'+run_tok+'<br><br>'+"Un Processed Links Due to Over Load <br>" 
            for nn in range(0,un_processing_link_numb):
                res = res + '<br>' + un_processing_link[nn]
        #res="Un Processed Links Due to Over Load <br>" + res
		
            return res#+temp_count_1+'<br><br>'+str(remaining_links)
            
    else:
        return "Please Enter Vaild Data"
        #er="Please provide same number of API_KEY and PROJECT_TOKEN"
        #return er

@app.route('/get_data', methods=['POST'])
def get_data():
    mycursor = mydb.cursor()
    mycursor.execute("CREATE TABLE IF NOT EXISTS scrap_data (id INT AUTO_INCREMENT PRIMARY KEY, product_name VARCHAR(800), product_url VARCHAR(800), runt VARCHAR(25))")

    v=[]
    p_url=[]
    p_name=[]
    
    selected_run = request.form['runt_drop']
    
    #return str(selected_run)
    mycursor = mydb.cursor()
    sql = "SELECT DISTINCT api FROM input_data WHERE runt = %s"
    adr = (selected_run,)
    mycursor.execute(sql, adr)
    api = mycursor.fetchall()
    for t in api:
        api1=t[0]
    mycursor = mydb.cursor()
    sql = "SELECT DISTINCT runt FROM scrap_data WHERE runt LIKE '%"+selected_run+"%'"
    mycursor.execute(sql)
    store_var = mycursor.fetchall()
    #return str(store_var)
    if len(store_var) == 0:
        if selected_run != "":
            params = {"api_key": api1,"format": "json"}
            r = requests.get('https://www.parsehub.com/api/v2/runs/'+selected_run+'/data', params=params)
            #r = requests.get('https://www.parsehub.com/api/v2/projects/'+pt1[ii]+'/last_ready_run/data', params=params)
            y = json.loads(r.text)
            for n in range (0,len(y['details'])):
                for nn in range (0,len(y['details'][n]['selection1'])):
                    if "name" in (y['details'][n]['selection1'][nn]):
                        p_name.append(y['details'][n]['selection1'][nn]['name'])
                    else:
                        p_name.append("")
                for nn in range (0,len(y['details'][n]['selection1'])):
                    if "url" in y['details'][n]['selection1'][nn]:
                        p_url.append(y['details'][n]['selection1'][nn]['url'])
                    else:
                        p_url.append("")

            p_len=len(p_name)        
            for i in range(0,p_len):
                v.append((p_name[i],p_url[i],selected_run))
            
            def generate():
                data = StringIO()
                w = csv.writer(data)

                # write header
                w.writerow(('Product_name', 'Product_URL'))
                yield data.getvalue()
                data.seek(0)
                data.truncate(0)
                # write each log item
                for item in v:
                    w.writerow((
                        item[0],
                        item[1]#.isoformat()  # format datetime as string
                        ))
                    yield data.getvalue()
                    data.seek(0)
                    data.truncate(0)
                # stream the response as the data is generated

            response = Response(generate(), mimetype='text/csv')
        
            sql = "INSERT INTO scrap_data (product_name,product_url,runt) VALUES (%s, %s, %s)"
            mycursor.executemany(sql, v)
            mydb.commit()
        
            response.headers.set("Content-Disposition", "attachment", filename=now1)
            return response
        else:
            return "Please Select Proper Run Token"
    else:
        if selected_run != "":
            params = {"api_key": api1,"format": "json"}
            r = requests.get('https://www.parsehub.com/api/v2/runs/'+selected_run+'/data', params=params)
            #r = requests.get('https://www.parsehub.com/api/v2/projects/'+pt1[ii]+'/last_ready_run/data', params=params)
            y = json.loads(r.text)
            for n in range (0,len(y['details'])):
                for nn in range (0,len(y['details'][n]['selection1'])):
                    if "name" in (y['details'][n]['selection1'][nn]):
                        p_name.append(y['details'][n]['selection1'][nn]['name'])
                    else:
                        p_name.append("")
                for nn in range (0,len(y['details'][n]['selection1'])):
                    if "url" in y['details'][n]['selection1'][nn]:
                        p_url.append(y['details'][n]['selection1'][nn]['url'])
                    else:
                        p_url.append("")

            p_len=len(p_name)        
            for i in range(0,p_len):
                v.append((p_name[i],p_url[i],selected_run))
            
            def generate():
                data = StringIO()
                w = csv.writer(data)

                # write header
                w.writerow(('Product_name', 'Product_URL'))
                yield data.getvalue()
                data.seek(0)
                data.truncate(0)
                # write each log item
                for item in v:
                    w.writerow((
                        item[0],
                        item[1]#.isoformat()  # format datetime as string
                        ))
                    yield data.getvalue()
                    data.seek(0)
                    data.truncate(0)
                # stream the response as the data is generated

            response = Response(generate(), mimetype='text/csv')
            response.headers.set("Content-Disposition", "attachment", filename=now1)
            return response
        else:
            return "Please Select Proper Run Token"

if __name__ == '__main__':
    app.run()    
