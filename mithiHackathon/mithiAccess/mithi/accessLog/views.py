from django.shortcuts import render
from django.http import HttpResponse
from django.views.generic import TemplateView
from nltk.tokenize import SpaceTokenizer
import mysql.connector
import json
import math





def connect():
    mydb = mysql.connector.connect(
    host="192.168.43.201",
    #   port = "3306",
    user="demon",
    passwd="asd123",
    database = "access_log"
    )

    return mydb
    
mydb = connect()
mycursor = mydb.cursor() 



#Open file and read lines
def readfile(path):
    connect()
    file = open(path, "r")
    return file

def convert_date(date_time):
    month = {
                'Jan' : '01',
                'Feb' : '02',
                'Mar' : '03',
                'Apr' : '04',
                'May' : '05',
                'Jun' : '06',
                'Jul' : '07',
                'Aug' : '08',
                'Sep' : '09',
                'Oct' : '10',
                'Nov' : '11',
                'Dec' : '12',
            }
    
    datetime = date_time[7:11]+"-"+month[date_time[3:6]]+"-"+date_time[0:2]+" "+date_time[12:]
    return datetime
    
    
    

def process(tokens):
    
    
    try:
        ip = tokens[0]
        date_time = tokens[3][1:]
        date_time = convert_date(date_time)
        method = tokens[5][1:]
        url = tokens[6]
        protocol = tokens[7][:-1]
        status = tokens[8]
        size = tokens[9][:-1]
    except:
        method = tokens[5][1:-1]
        url = tokens[5][1:-1]
        protocol = tokens[5][1:-1]
        status = tokens[6]
        size = tokens[7][:-1]
#     print(ip + ' ' + date_time + ' ' + method + ' ' + url + ' ' + protocol + ' ' + status + ' ' + size)

    ctype = 'Others'
    if "?" in url:
        ctype = 'Query'
    elif (".jpg" in url) or (".JPG" in url) or (".gif" in url) or (".png" in url) or (".cgi" in url) or (".ico" in url) or (".svg" in url):
        ctype = 'Image'
    elif (".html" in url) or (".webHome" in url) or (".js" in url) or (".xml" in url) or (".css" in url):
        ctype = 'Website'

    sql = "INSERT INTO logs_c (IP,date_time,method,url,protocol,response,size,category) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
    val = (ip,date_time,method,url,protocol,status,size,ctype)
    mycursor.execute(sql, val)

    
    
# Type 0 -> Tab Seperated (Server 1)
# Type 1 -> Space Seperated (Server 2)

def query_1():
    # Top client ip addresses by number of requests
    sql = "SELECT IP, count(*) FROM logs_c GROUP BY IP ORDER BY count(*) DESC LIMIT 5";
    mycursor.execute(sql)
    results = mycursor.fetchall()
    res = []
    for x in results:
        res.append(list(x))
    # res = json.dumps(res)
    return res

def query2():
    sql = "SELECT URL, count(*) FROM logs_c GROUP BY URL ORDER BY count(*) DESC LIMIT 5";
    mycursor.execute(sql)
    results = mycursor.fetchall()
    result = []
    for x in results:
        result.append(list(x))
    return result

def query3():
    sql = "SELECT URL, sum(size) FROM logs_c GROUP BY URL ORDER BY sum(size) DESC LIMIT 5";
    mycursor.execute(sql)
    results = mycursor.fetchall()
    result = []
    for x in results:
        result.append(list(x))
    return result

def query4():
    sql = "SELECT method, count(*) FROM logs_c GROUP BY method ORDER BY count(*) DESC LIMIT 5";
    mycursor.execute(sql)
    results = mycursor.fetchall()
    result=[]
    for x in results:
        result.append(list(x))
        
    return result

def query5():
    sql = "SELECT category, count(*) FROM logs_c GROUP BY category ORDER BY count(*) DESC";
    mycursor.execute(sql)
    results = mycursor.fetchall()
    result=[]
    for x in results:
        result.append(list(x))
    return result

def query7():
    result = []
    sql = "SELECT sum(size) FROM logs_c WHERE method = \"GET\" OR method = \"HEAD\" OR method = \"OPTIONS\"";
    mycursor.execute(sql)
    download = mycursor.fetchall()
    x = list(download[0])
    
    result.append(math.ceil(x[0]))
    sql = "SELECT sum(size) FROM logs_c WHERE method = \"POST\"";
    mycursor.execute(sql)
    upload = mycursor.fetchall()
    x = list(upload[0])
    result.append(math.ceil(x[0]))

    return result

class HomePageView(TemplateView):
    template_name = 'home.htm'

class DisplayPageView(TemplateView):
    template_name = 'display.htm'

def displayPageView(request):
    mycursor.execute('TRUNCATE table logs_c')
    filePath = request.GET['input-file']
    filePath = "C:/Users/Rhishabh/Documents/mithi hackathon/" + filePath
    log = readfile(filePath)

    line = log.readline()
    tk = SpaceTokenizer()
    tokens = tk.tokenize(line)
    while line:
        tokens = tk.tokenize(line)
        process(tokens)
        line = log.readline()

    mydb.commit()
    
    result1 = query_1()
    result2 = query2()
    result3 = query3()
    result4 = query4()
    result5 = query5()
    result7 = query7()

    # mydb.close()
    temp = [['test', 'test'], ['test', 'test']]
    test = 'sdsds'
    return render(request, 'display.htm', {'ipfile': filePath, 'result1': result1, 'result2': result2, 'result3': result3, 'result4': result4, 'result5': result5, 'result7': result7})

def homePageView(request):
    return HttpResponse('Hello, World!')
