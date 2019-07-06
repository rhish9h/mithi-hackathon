from nltk.tokenize import SpaceTokenizer
import mysql.connector

mydb = mysql.connector.connect(
  host="192.168.43.201",
#   port = "3306",
  user="demon",
  passwd="asd123",
  database = "access_log"
)

mycursor = mydb.cursor()
sql = "INSERT INTO logs_c (IP,date_time,method,url,protocol,response,size) VALUES (%s,%s,%s,%s,%s,%s,%s)"

#Open file and read lines
def readfile(path):
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
           
    
    val = (ip,date_time,method,url,protocol,status,size)
    mycursor.execute(sql, val)

    
    
# Type 0 -> Tab Seperated (Server 1)
# Type 1 -> Space Seperated (Server 2)


log = readfile("access_log")

line = log.readline()
tk = SpaceTokenizer()
tokens = tk.tokenize(line)

while line:
    tokens = tk.tokenize(line)
    process(tokens)
    line = log.readline()

    
mydb.commit()
print("records inserted.")

# Top client ip addresses by number of requests
sql = "SELECT IP, count(*) FROM logs_c GROUP BY IP ORDER BY count(*) DESC LIMIT 5";
mycursor.execute(sql)
results = mycursor.fetchall()

for x in results:
    print(x)

#ii. Top urls by number of requests    

print("\n\n\n")

sql = "SELECT URL, count(*) FROM logs_c GROUP BY URL ORDER BY count(*) DESC LIMIT 5";
mycursor.execute(sql)
results = mycursor.fetchall()

for x in results:
    print(x)

# iii. Top urls by size of response
print("\n\n\n")
sql = "SELECT URL, size FROM logs_c GROUP BY URL ORDER BY sum(size) DESC LIMIT 5";
mycursor.execute(sql)
results = mycursor.fetchall()

for x in results:
    print(x)



# iv. Top http request methods by number of requests
print("\n\n\n")
sql = "SELECT method, count(*) FROM logs_c GROUP BY method ORDER BY count(*) DESC LIMIT 5";
mycursor.execute(sql)
results = mycursor.fetchall()

for x in results:
    print(x)



# v. Top Content types by number of request


# vi. Trend of rate of http request per 5 mins


# vii. Total data transfer uploaded and downloaded.
print("\n\n\n")
sql = "SELECT sum(size) FROM logs_c WHERE method = \"GET\" OR method = \"HEAD\" OR method = \"OPTIONS\"";
mycursor.execute(sql)
download = mycursor.fetchall()
print(download)
sql = "SELECT sum(size) FROM logs_c WHERE method = \"POST\"";
mycursor.execute(sql)
upload = mycursor.fetchall()
print(upload)





mycursor.execute("TRUNCATE table logs_c")
mydb.close()
