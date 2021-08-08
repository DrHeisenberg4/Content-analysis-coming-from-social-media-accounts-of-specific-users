import pymysql
import pandas as pd
import sys
import facebook_scraper
import fb_backend

connection=fb_backend.connectionopen()
cursor= fb_backend.connectioncursor(connection)
try:
    df = pd.read_csv(r'C:\Users\steli\Desktop\Thesis\usernames.csv', encoding = 'ISO-8859-7', delimiter=';')
    print("Usernames have been loaded!\n")
except:
    print ("Usernames file error!\n")
    df = []
counter = 0
for facebook_user in df["Facebook"]:
    post_list = fb_backend.fb_scraper(facebook_user)
    alias = df.iloc[counter, 0]
    fb_backend.RecordFBValuesToDB(facebook_user, post_list, alias, cursor, connection)
    counter = counter + 1
fb_backend.connectionclose(connection)


