from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
import time
import json
import re
import pymysql

def connectionopen():
    try:
        dbconnection = pymysql.connect(host='localhost', user='root', password='steliosgeo1997', database='socialmediadb', charset='utf8')
        print("Ekana sindesi me tin vasi!\n")
    except Exception as e:
        print("Connection error!\n")
    return dbconnection

def connectioncursor(connection):
    cursor = connection.cursor()
    cursor.execute('SET NAMES utf8mb4')
    cursor.execute("SET CHARACTER SET utf8mb4")
    return cursor

def connectionclose(connection):
    connection.close()

def insertInstagramUserId(alias,user_id,cursor,connection):
    sql = "UPDATE USER SET instagram_user_id = '%s' WHERE Alias = '%s'" % (int(user_id), str(alias))

    try:
        cursor.execute(sql)
        connection.commit()
        print("Evala to id ston user! \n")
    except:
        connection.rollback()
        print("DEN katafera na valw to id ston user! \n")


def insertValueInstaUsers(user_id, instagram_user ,followers, posts, cursor , connection):
  sql = """INSERT INTO Instagram (instagram_user_id, instagram_username, followers, posts) \
   VALUES ('%s', '%s', '%d', '%d')""" % \
        (str(user_id),str(instagram_user), int(followers), int(posts))

  try:
    cursor.execute(sql)
    connection.commit()
    print("O xristis mpike stin vasi!\n")
  except:
    connection.rollback()
    print("O xristis DEN mpike stin vasi!\n")

def updatevalueInstausers(user_id , instagram_user, followers, posts, cursor,connection):
    sql3 = """UPDATE Instagram SET instagram_username = '%s' ,followers = '%d', posts = '%d' WHERE instagram_user_id = '%d' 
    """ % \
          (str(instagram_user), int(followers), int(posts),int(user_id))
    try:
        cursor.execute(sql3)
        connection.commit()

    except:
        connection.rollback()


def insertValueInstaPosts(post_id, user_id, text, likes, comments, cursor , connection):
  sql = """INSERT INTO Instagram_Posts (instagram_post_id, instagram_user_id, likes, comments, text) \
	VALUES ('%s', '%s', '%d', '%d', '%s')""" % \
	(str(post_id), str(user_id), int(likes), int(comments), str(text))

  try:
    cursor.execute(sql)
    connection.commit()
    print("To post mpike stin vasi!\n")
  except:
    connection.rollback()
    print("To post DEN mpike stin vasi!\n")

def updatevalueinstaposts(post_id, likes, comments, cursor, connection):
    sql = """UPDATE Instagram_Posts SET likes = '%d' ,comments = '%d' WHERE instagram_post_id = '%d'
    	""" % \
           (int(likes), int(comments), int(post_id))
    try:
        cursor.execute(sql)
        connection.commit()
        print("To insta+ post egine update!\n")
    except:
        connection.rollback()
        print("To insta post DEN egine update!\n")

def deEmojify(string):
    out_string = ""
    array = string.split()
    for word in array:
        out_string += re.sub(r'[\W]', '', word) + " "
    #out_text = out_string.replace("\'", "")
    return out_string

def cleantextfromquotes(string):
    finaltext=re.sub(r'\'', '', string)
    return finaltext


def extract_hashtags(id,text,cursor,connection):
    hashtag_list = []
    for word in text.split():
        if word[0] == '#':
            hashtag_list.append(word[1:])
    for hashtag in hashtag_list:
        print(hashtag)


def insertvalueinstamedia(media_text,cursor,connection):
  sql = """INSERT INTO Media (media_text) \
	VALUES ('%s')""" % \
	(str(media_text))
  try:
      cursor.execute(sql)
      connection.commit()
  except:
      connection.rollback()

def insertvalueinstahashtag(value,cursor,connection):
  sql = """INSERT INTO Hashtag (value) \
	VALUES ('%s') """ % \
	(value)
  try:
      cursor.execute(sql)
      connection.commit()
  except:
      connection.rollback()

def insertvaluemedia2insta(instapost_id,media_id,cursor,connection):
  sql = """INSERT INTO Media2Insta (instagram_post_id,media_id) \
	VALUES ('%d','%d')""" % \
	(int(instapost_id),int(media_id))
  try:
      cursor.execute(sql)
      connection.commit()
  except:
      connection.rollback()


def insertvaluehash2insta(instapost_id,hash_id,cursor,connection):
  sql = """INSERT INTO Hash2Insta (instagram_post_id,hash_id) \
	VALUES ('%d','%d')""" % \
	(int(instapost_id),int(hash_id))
  try:
      cursor.execute(sql)
      connection.commit()
  except:
      connection.rollback()

def RecordInstaValuesToDB(instagram_user, alias , cursor , connection, post_hrefs, driver):
    cursor.execute("SELECT * FROM Instagram")
    myresult = cursor.fetchall()
    exist_user = False
    for exist_users in myresult:
        if(instagram_user == exist_users[1]):
            exist_user = True
        else:
            exist_user = False



    for i in range (0, len(post_hrefs)):
        time.sleep(20)
        postlink = post_hrefs[i]
        dict = getjsonofpost(driver, postlink)
        if (dict == 0):
            continue
        post_id,user_id,likes,comments,text,followers,posts = getpostmetadata(dict)
        if (i == 0):
            if (exist_user == False):
                insertValueInstaUsers(user_id, instagram_user, followers, posts, cursor, connection)
                insertInstagramUserId(alias, user_id, cursor, connection)
            else:
                updatevalueInstausers(user_id, instagram_user, followers, posts, cursor, connection)


        exist_post = False
        sql = """SELECT * FROM Instagram_Posts WHERE instagram_user_id = '%s'""" % \
              (user_id)
        cursor.execute(sql)
        myresult = cursor.fetchall()
        for exist_posts in myresult:
            try:
                if(int(post_id) == int(exist_posts[0])):
                    exist_post = True
                    break
                else:
                    exist_post = False
            except:
                exist_post = False

        if(exist_post == False):
           try:
                insertValueInstaPosts(post_id, user_id, text, likes, comments, cursor, connection)
                getpostmedia(dict,post_id,cursor,connection)
                getposthashtags(post_id, driver, cursor, connection)
           except:
                print("To post den mporese na mpei sti vasi!\n")
        else:
            print("To insta post iparxei idi stin vasi,paw na to kanw update!\n")
            updatevalueinstaposts(post_id, likes, comments, cursor, connection)



def openwebdriver():
    driver = webdriver.Chrome('C:/Users/steli/Downloads/chromedriver_win32/chromedriver.exe')
    time.sleep(5)
    driver.get("https://www.instagram.com/")
    accept_all = WebDriverWait(driver, 20).until(
        EC.element_to_be_clickable((By.XPATH, '//button[contains(text(), "Accept All")]'))).click()
    time.sleep(5)
    username = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "input[name='username']")))
    password = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "input[name='password']")))
    username.clear()
    username.send_keys("stelios_georgilas")
    time.sleep(5)
    password.clear()
    password.send_keys("steliosgeo19971997")
    time.sleep(5)
    button = WebDriverWait(driver, 30).until(
        EC.element_to_be_clickable((By.CSS_SELECTOR, "button[type='submit']"))).click()
    not_now = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH, '//button[contains(text(), "Not Now")]'))).click()
    time.sleep(2)
    not_now2 = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH, '//button[contains(text(), "Not Now")]'))).click()
    return driver


def searchforuser(username,driver):
    time.sleep(10)
    searchbox = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH, "//input[@placeholder='Search']")))
    searchbox.clear()
    searchbox.send_keys(username)
    time.sleep(3)
    searchbox.send_keys(Keys.ENTER)
    time.sleep(3)
    searchbox.send_keys(Keys.ENTER)
    time.sleep(3)

def scrolldown(driver):
    # scroll down to scrape more images
    driver.execute_script("window.scrollTo(0, 1000);")
    time.sleep(5)
    driver.execute_script("window.scrollTo(0, 4000);")
    time.sleep(5)
    driver.execute_script("window.scrollTo(0, 4000);")
    time.sleep(5)
    driver.execute_script("window.scrollTo(0, 4000);")
    time.sleep(5)
    driver.execute_script("window.scrollTo(0, 3500);")
    time.sleep(5)

def takepostlinks(driver):
    post_xpath_str = "//a[contains(@href, '/p/')]"
    post_links = driver.find_elements_by_xpath(post_xpath_str)
    post_hrefs = []
    if len(post_links) > 0:
        for i in range(0, len(post_links)):
            post_link_el = post_links[i]
            if post_link_el != None:
                post_hrefs.append(post_link_el.get_attribute("href"))
                print(post_hrefs[i])
    time.sleep(10)
    return post_hrefs


def getjsonofpost(driver,post_href):
    driver.get(post_href)
    time.sleep(10)
    source_data = driver.page_source
    JSON = re.compile("\"graphql\":(\\{.+\\})", re.DOTALL)
    matches = JSON.search(source_data)
    html_text = matches.group(1)
    text1 = html_text.split('}}}]},')[0]
    text2 = '}}}]}}}'
    text_i_want = text1+text2
    try:
        dict = json.loads(text_i_want)
    except:
        return 0
    return dict

def getpostmetadata(dict):
    post_id = dict["shortcode_media"]["id"]
    user_id = dict["shortcode_media"]["owner"]["id"]
    likes = dict["shortcode_media"]["edge_media_preview_like"]["count"]
    comments= dict["shortcode_media"]["edge_media_to_parent_comment"]["count"]
    try:
        text= dict["shortcode_media"]["edge_media_to_caption"]["edges"][0]["node"]["text"]
        text1= cleantextfromquotes(text)
        finaltext = deEmojify(text1)
    except:
        finaltext = "Empty text"
    followed = dict["shortcode_media"]["owner"]["edge_followed_by"]
    ffollowed = str(followed)
    followed1 =  ffollowed.split(' ')[1]
    followers = followed1.split('}')[0]
    nposts = dict["shortcode_media"]["owner"]["edge_owner_to_timeline_media"]
    npostss = str(nposts)
    nposts1 = npostss.split(' ')[1]
    posts = nposts1.split('}')[0]
    return post_id,user_id,likes,comments,finaltext,followers,posts


def getpostmedia(dict,post_id,cursor,connection):
    try:
        for i in range(0, len(dict["shortcode_media"]["edge_sidecar_to_children"]["edges"])):
            mediaurl = dict["shortcode_media"]["edge_sidecar_to_children"]["edges"][i]["node"]["display_url"]
            sqlmedia = """SELECT media_id FROM Media WHERE media_text = '%s'""" % \
                       (str(mediaurl))
            cursor.execute(sqlmedia)
            myresultmedia = cursor.fetchone()
            if myresultmedia == None:
                try:
                    insertvalueinstamedia(mediaurl, cursor, connection)
                    sqlmediaid = """SELECT media_id FROM Media WHERE media_text = '%s'""" % \
                                 (str(mediaurl))
                    cursor.execute(sqlmediaid)
                    myresultmediaid = cursor.fetchone()
                    insertvaluemedia2insta(post_id, myresultmediaid[0], cursor, connection)
                except:
                    print("Error at media insertion!\n")
            else:
                insertvaluemedia2insta(post_id, myresultmedia[0], cursor, connection)
    except:
        if(dict["shortcode_media"]["is_video"] == True):
            mediaurl = dict["shortcode_media"]["video_url"]
            sqlmedia = """SELECT media_id FROM Media WHERE media_text = '%s'""" % \
                       (str(mediaurl))
            cursor.execute(sqlmedia)
            myresultmedia = cursor.fetchone()
            if myresultmedia == None:
                try:
                    insertvalueinstamedia(mediaurl, cursor, connection)
                    sqlmediaid = """SELECT media_id FROM Media WHERE media_text = '%s'""" % \
                                 (str(mediaurl))
                    cursor.execute(sqlmediaid)
                    myresultmediaid = cursor.fetchone()
                    insertvaluemedia2insta(post_id, myresultmediaid[0], cursor, connection)
                except:
                    print("To media den mpike.\n")
            else:
                insertvaluemedia2insta(post_id, myresultmedia[0], cursor, connection)
        else:
            mediaurl = dict["shortcode_media"]["display_url"]
            sqlmedia = """SELECT media_id FROM Media WHERE media_text = '%s'""" % \
                       (str(mediaurl))
            cursor.execute(sqlmedia)
            myresultmedia = cursor.fetchone()
            if myresultmedia == None:
                try:
                    insertvalueinstamedia(mediaurl, cursor, connection)
                    sqlmediaid = """SELECT media_id FROM Media WHERE media_text = '%s'""" % \
                                 (str(mediaurl))
                    cursor.execute(sqlmediaid)
                    myresultmediaid = cursor.fetchone()
                    insertvaluemedia2insta(post_id, myresultmediaid[0], cursor, connection)
                except:
                    print("To media den mpike.\n")
            else:
                insertvaluemedia2insta(post_id, myresultmedia[0], cursor, connection)
def getposthashtags(post_id,driver,cursor,connection):
    hashtag_xpath = "//a[contains(@href, '/explore/tags')]"
    time.sleep(5)
    post_hashtags = driver.find_elements_by_xpath(hashtag_xpath)
    if len(post_hashtags) > 0:
        for i in range(0, len(post_hashtags)):
            post_hashtag = post_hashtags[i]
            if post_hashtag != None:
                hashtagref = post_hashtag.get_attribute("href")
                hashtag1 = hashtagref.split('tags/')[1]
                hashtag = hashtag1.split('/')[0]
                sqlhashid1 = """SELECT hash_id FROM Hashtag WHERE value = '%s'""" % \
                           (str(hashtag))
                cursor.execute(sqlhashid1)
                myresulthashid = cursor.fetchone()
                if myresulthashid == None:
                    try:
                        insertvalueinstahashtag(hashtag, cursor, connection)
                        sqlhashid2 = """SELECT hash_id FROM Hashtag WHERE value = '%s'""" % \
                                     (str(hashtag))
                        cursor.execute(sqlhashid2)
                        myresulthashid1 = cursor.fetchone()
                        insertvaluehash2insta(post_id, myresulthashid1[0], cursor, connection)
                    except:
                        print("To hashtag den mpike.\n")
                else:
                    insertvaluehash2insta(post_id, myresulthashid[0], cursor, connection)