import pandas as pd
import instagram_backend

connection=instagram_backend.connectionopen()
cursor= instagram_backend.connectioncursor(connection)
try:
    df = pd.read_csv(r'C:\Users\steli\Desktop\Thesis\usernames.csv', encoding = 'ISO-8859-7', delimiter=';')
    print("Usernames have been loaded!\n")
except:
    print ("Usernames file error!\n")
    df = []
counter = 0
driver = instagram_backend.openwebdriver()
for instagram_user in df["Instagram"]:
    alias = df.iloc[counter, 0]
    instagram_backend.searchforuser(instagram_user, driver)
    instagram_backend.scrolldown(driver)
    post_hrefs = instagram_backend.takepostlinks(driver)
    instagram_backend.RecordInstaValuesToDB(instagram_user, alias, cursor, connection, post_hrefs, driver)
    counter = counter + 1
instagram_backend.connectionclose(connection)



