import pymysql
import pandas as pd
import sys
import re
import facebook_scraper
import time

def connectionopen():
    print("MPIKA EDW\n")
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

def fb_scraper(fb_user):
    get_posts = facebook_scraper.get_posts(fb_user, pages=16,timeout=10)
    post_list = []
    for post in get_posts:
        print(post)
        post_list.append(post)
    return post_list

def fb_profile(fb_user):
    get_profile = facebook_scraper.get_profile(fb_user)
    return get_profile

def insertfacebookuserid(alias,user_id,cursor,connection):
    sql = "UPDATE USER SET facebook_user_id = '%s' WHERE Alias = '%s'" % (int(user_id), str(alias))

    try:
        cursor.execute(sql)
        connection.commit()
        print("Evala to id ston user! \n")
    except:
        connection.rollback()
        print("DEN katafera na valw to id ston user! \n")

def insertValueFBUsers(user_id, fb_user , cursor , connection):
  val=(user_id, fb_user , None)
  sql = """INSERT INTO Facebook (facebook_user_id, facebook_username, friends) VALUES (%s, %s, %s)"""

  try:
    cursor.execute(sql,val)
    connection.commit()
    print("O xristis mpike stin vasi!\n")
  except:
    connection.rollback()
    print("O xristis DEN mpike stin vasi!\n")

def insertValueFBPosts(post_id, user_id, text, likes, comments, shares, date, cursor , connection):
  sql = """INSERT INTO FacebookPosts (facebook_post_id, facebook_user_id, likes, comments, shares, createdat ,plaintext) \
	VALUES ('%s', '%s', '%d', '%d', '%d','%s','%s')""" % \
	(str(post_id), str(user_id), likes, comments, shares, str(date),str(text))

  try:
    cursor.execute(sql)
    connection.commit()
    print("To post mpike stin vasi!\n")
  except:
    connection.rollback()
    print("To post DEN mpike stin vasi!\n")


def updatevaluefbposts(post_id, likes, comments, shares, cursor, connection):
    sql = """UPDATE FacebookPosts SET likes = '%d' ,comments = '%d' , shares = '%d' WHERE facebook_post_id = '%d'
    	""" % \
           (int(likes), int(comments), int(shares), int(post_id))
    try:
        cursor.execute(sql)
        connection.commit()
        print("To fb post egine update!\n")
    except:
        connection.rollback()
        print("To fb post DEN egine update!\n")

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

def cleantextfromq(string):
    fintext=re.sub(r'\’', ' ', string)

def extract_hashtags(id,text,cursor,connection):
    hashtag_list = []
    for word in text.split():
        if word[0] == '#':
            hashtag_list.append(word[1:])
    for hashtag in hashtag_list:
        print(hashtag)
        sqlhashtag = """SELECT hash_id FROM Hashtag WHERE value = '%s' """ % \
                     (hashtag)
        cursor.execute(sqlhashtag)
        myresulthashtag = cursor.fetchone()
        if myresulthashtag == None:
            try:
                insertvaluefbhashtag(hashtag, cursor, connection)
                sqlhashtagid = """SELECT hash_id FROM Hashtag WHERE value = '%s' """ % \
                               (hashtag)
                cursor.execute(sqlhashtagid)
                myresulthashtagid = cursor.fetchone()
                insertvaluehash2fb(id, myresulthashtagid[0], cursor, connection)
            except:
                print("Error on hashtag insertion!\n")
        else:
            insertvaluehash2fb(id, myresulthashtag[0], cursor, connection)



def insertvaluefblinks(url , domain,cursor,connection):
  sql = """INSERT INTO Links (url,domain) \
	VALUES ('%s', '%s')""" % \
	(str(url), str(domain))
  try:
      cursor.execute(sql)
      connection.commit()
  except:
      connection.rollback()


def insertvaluefbmedia(media_text,cursor,connection):
  sql = """INSERT INTO Media (media_text) \
	VALUES ('%s')""" % \
	(str(media_text))
  try:
      cursor.execute(sql)
      connection.commit()
  except:
      connection.rollback()


def insertvaluefbhashtag(value,cursor,connection):
  sql = """INSERT INTO Hashtag (value) \
	VALUES ('%s') """ % \
	(value)
  try:
      cursor.execute(sql)
      connection.commit()
  except:
      connection.rollback()

def insertvaluelinks2fb(fbpost_id,link_id,cursor,connection):
  sql = """INSERT INTO Links2Fb (facebook_post_id,link_id) \
	VALUES ('%d','%d')""" % \
	(int(fbpost_id),int(link_id))
  try:
      cursor.execute(sql)
      connection.commit()
  except:
      connection.rollback()


def insertvaluemedia2fb(fbpost_id,media_id,cursor,connection):
  sql = """INSERT INTO Media2Fb (facebook_post_id,media_id) \
	VALUES ('%d','%d')""" % \
	(int(fbpost_id),int(media_id))
  try:
      cursor.execute(sql)
      connection.commit()
  except:
      connection.rollback()


def insertvaluehash2fb(fbpost_id,hash_id,cursor,connection):
  sql = """INSERT INTO Hash2Fb (facebook_post_id,hash_id) \
	VALUES ('%d','%d')""" % \
	(int(fbpost_id),int(hash_id))
  try:
      cursor.execute(sql)
      connection.commit()
  except:
      connection.rollback()


def RecordFBValuesToDB(fb_user, post_list , alias , cursor , connection):
    user_id = post_list[0]['user_id']
    cursor.execute("SELECT * FROM Facebook")
    myresult = cursor.fetchall()
    exist_user = False
    for exist_users in myresult:
        if(fb_user == exist_users[1]):
            exist_user = True
        else:
            exist_user = False

    if(exist_user == False):
        insertValueFBUsers(user_id, fb_user, cursor, connection)
        insertfacebookuserid(alias, user_id, cursor, connection)
    else:
        print("User is already loaded in database!\n")




    for i in range (0, len(post_list)):
        time.sleep(2)
        post = post_list[i]
        exist_post = False
        sql = """SELECT * FROM FacebookPosts WHERE facebook_user_id = '%s'""" % \
              (user_id)
        cursor.execute(sql)
        myresult = cursor.fetchall()
        for exist_posts in myresult:
            try:
                if(int(post['post_id']) == int(exist_posts[0])):
                    exist_post = True
                    break
                else:
                    exist_post = False
            except:
                exist_post = False

        if(exist_post == False):
            text=post['text']
            try:
                cleantext=cleantextfromquotes(text)
            except:
                continue
            try:
                insertValueFBPosts(str(post['post_id']), user_id, str(cleantext), int(post['likes']), int(post['comments']), int(post['shares']),
                                   post['time'].strftime("%Y/%m/%d %H:%M:%S"),cursor, connection)


                if (post['link']!= None):
                        link = post['link']
                        try:
                            b = cleantextfromquotes(link)
                            sqllink = """SELECT link_id FROM Links WHERE url = '%s'""" % \
                                    (b)
                            cursor.execute(sqllink)
                            myresultlink = cursor.fetchone()
                            if myresultlink == None:
                                try:
                                    urlsplit = b.split("//", 1)
                                    domainlink = urlsplit[1]
                                    domainsplit = domainlink.split("/", 1)
                                    domain = domainsplit[0]
                                    insertvaluefblinks(b, domain, cursor, connection)
                                    sqllinkid = """SELECT link_id FROM Links WHERE url = '%s'""" % \
                                                (str(b))
                                    cursor.execute(sqllinkid)
                                    myresultlinkid = cursor.fetchone()
                                    insertvaluelinks2fb(post['post_id'], myresultlinkid[0], cursor, connection)
                                except: print("Error at link insertion to database!\n")
                        except: print("Error cleaning quotes!\n")
                        else:
                            insertvaluelinks2fb(post['post_id'], myresultlink[0], cursor, connection)


                if (post['image']!= None):
                    mediaurl = post['image']
                    sqlmedia = """SELECT media_id FROM Media WHERE media_text = '%s'""" % \
                                (str(mediaurl))
                    cursor.execute(sqlmedia)
                    myresultmedia = cursor.fetchone()
                    if myresultmedia == None:
                        try:
                            insertvaluefbmedia(mediaurl, cursor, connection)
                            sqlmediaid = """SELECT media_id FROM Media WHERE media_text = '%s'""" % \
                                            (str(mediaurl))
                            cursor.execute(sqlmediaid)
                            myresultmediaid = cursor.fetchone()
                            insertvaluemedia2fb(post['post_id'], myresultmediaid[0], cursor, connection)
                        except:
                            print("Error on media insertion to database!\n")
                    else:
                        insertvaluemedia2fb(post['post_id'], myresultmedia[0], cursor, connection)


                text1 = post['text']
                id = post['post_id']
                extract_hashtags(id,text1,cursor,connection)
            except:
                print("Error on post insertion to database!\n")
        else:
            print("Post already loaded in database,waiting for update!\n")
            updatevaluefbposts(str(post['post_id']), post['likes'], post['comments'], post['shares'], cursor, connection)

