import pandas as pd
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import backend

connection=backend.connectionopen()
cursor= backend.connectioncursor(connection)
try:
    df = pd.read_csv(r'C:\Users\steli\Desktop\Thesis\usernames.csv', encoding = 'ISO-8859-7', delimiter=';')
    print("Usernames have been loaded!\n")
except:
    print ("Usernames file error!\n")
    df = []
api = backend.twitter_api_authorization()
counter=0
for twitter_user in df["Twitter"]:
    alias=df.iloc[counter,0]
    backend.twitter_fetch_data(alias,api, twitter_user,cursor,connection)
    counter = counter+1
backend.connectionclose(connection)


