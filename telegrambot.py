import requests 
import mysql.connector
from dotenv import load_dotenv
import os
load_dotenv()  # Load variables from .env


# telegram connection 
api_token = os.getenv('APITOKEN')
chatID = os.getenv('CHATID')
api_url = f"https://api.telegram.org/bot{api_token}/sendMessage"
# Database Connection
connection = mysql.connector.connect(
    host=os.getenv('HOST'),
    user = os.getenv('USER'),
    database = os.getenv('DATABASE')
)
mydb = connection.cursor()
def team_history():
    url = "https://sdp-prem-prod.premier-league-prod.pulselive.com/api/v1/competitions/8/teams?_limit=60"
    re = requests.get(url = url)
    data1 = re.json()['data']
    for i in data1:
                 
        params={
            "chat_id": chatID,
            "text": f"<b>{i['name']}</b> : ,<b>{i['stadium']['name']}</b>,{i['shortName']}, season: <b>{i['seasons']} </b>: " ,
            "parse_mode": "HTML"
        }
      
        r = requests.post(url=api_url, data=params)
        
team_history()


url = "https://sdp-prem-prod.premier-league-prod.pulselive.com/api/v1/competitions/8/seasons/2025/players"
next_data = None
sql_key= [] # to store the key for sql
sql_value2= [] # to store the value for sql
while(True):
    if next_data is None:
        params = f"?competitions=8&seasons=2025&limit=1"
    else:
        params = f"?competitions=8&seasons=2025&limit=100&_next={next_data}" 
    finalurl = url+params

    r = requests.get(url = finalurl)
    next_data = r.json()['pagination']['_next']
    data = r.json()['data']
    # print(data)


    for i in data:

        single_player_list = {} # why not list to replace the empty columns with None values for parameter matchmaking.
        for key,value in i.items():
                
            if type(value) == dict:
                          
                for key1,value1 in value.items():   

                    single_player_list[key1] = value1
                    if key1 not in sql_key:
                        sql_key.append(key1)
        
            else:
                single_player_list[key]= value
                if key not in sql_key:
                    sql_key.append(key)
        single_player_from_data= tuple(single_player_list.get(col, None) for col in sql_key) # to match the column with sql_key and replace with none if not found.
      
      
        
        sql_value2.append(single_player_from_data)   
         
        params={
            "chat_id": chatID,
            "text": f"<b>{i['currentTeam']['name']} </b>: <b>{i['name']['firstName']} </b>,{ i['name']['lastName']}" ,
            "parse_mode": "HTML"
        }
        r = requests.post(url=api_url, data=params) 
          
    if next_data is None:
        break

sql_key = tuple(sql_key) # for the immutable properties so column's doesnot change.

columns = ','.join(sql_key) # to make the columns in the format that (values) it treats tuple and '()' differently.
placeholder = ','.join(['%s'] * len(sql_key))
sql = f"Insert into player ({columns}) Values({placeholder})"
mydb.executemany(sql,sql_value2)
connection.commit()
connection.close()

 