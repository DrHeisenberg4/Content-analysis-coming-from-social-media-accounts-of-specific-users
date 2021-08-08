import pymysql
import tweepy
import sys
import numpy
import re
reload(sys)
sys.setdefaultencoding('utf-8')

def connectionopen():
    try:
        connection = pymysql.connect(host='localhost', user='root', password='steliosgeo1997', database='socialmediadb', charset='utf8')
        print("Connection with the database has been established!\n")
    except Exception as e:
        print("Connection error!\n")
    return connection

def connectioncursor(connection):
    cursor = connection.cursor()
    cursor.execute('SET NAMES utf8mb4')
    cursor.execute("SET CHARACTER SET utf8mb4")
    return cursor

def connectionclose(connection):
    connection.close()

def twitter_api_authorization():
    auth = tweepy.OAuthHandler("dhMKn3DmUcCctTiSFEbjOLcHZ", "NxPELlewWEcjXQXvNqb9NOKdWQb3oegHzjxNrqyY4XhG0fjZ09")
    auth.set_access_token("888163193167777793-2muwfSuT5pXvGj0h4MveIbaWFe3y2kG",
                          "o7FQ8h18ub6D3qJ73n7fHG49PG2TXi8MeK5Z4BViqZU6E")
    api = tweepy.API(auth,wait_on_rate_limit=True,
    wait_on_rate_limit_notify=True)
    return api







def twitter_fetch_data(alias,api, twitter_user,cursor,connection):
    user = api.get_user(twitter_user)
    user_id = user.id
    cursor.execute("SELECT * FROM Twitter")
    myresult = cursor.fetchall()
    exist_user = False
    for exist_users in myresult:
        if (user_id == exist_users[0]):
            exist_user = True
            break
        else:
            exist_user = False
    if(exist_user== False):
        insertvaluetwitterusers(user_id, user.screen_name, user.followers_count, user.friends_count, user.statuses_count, user.favourites_count,
                                user.description,cursor,connection)
        inserttwitteruserid(alias,user_id,cursor,connection)
    else:
        updatevaluetwitterusers(user_id, user.screen_name, user.followers_count, user.friends_count, user.statuses_count, user.favourites_count,
                                user.description,cursor,connection)

    for status in tweepy.Cursor(api.user_timeline, twitter_user, tweet_mode="extended").items():
        sql1 = """SELECT * FROM TWEETS WHERE twitter_user_id = '%d'""" % \
               (int(user_id))
        cursor.execute(sql1)
        myresult1 = cursor.fetchall()
        exist_tweet = False
        for exist_tweets in myresult1:
            try:
                if (int(status.id) == int(exist_tweets[0])):
                    exist_tweet = True
                    break
                else:
                    exist_tweet = False
            except:
                exist_tweet = False
        if (exist_tweet == False):
            text1 = status.full_text
            if (text1[0] == 'R' and text1[1] == 'T' and text1[3] == '@'):
                try:
                    insertvaluetwitterposts(status.id, status.user.id, status.retweet_count, status.favorite_count,
                                            status.created_at, status.retweeted_status.full_text,cursor,connection)
                    if 'user_mentions' in status.retweeted_status.entities:
                        for usermention in status.retweeted_status.entities["user_mentions"]:
                            mention = usermention["screen_name"]
                            insertvaluetwittermentions(status.id, mention,cursor,connection)
                    if 'urls' in status.retweeted_status.entities:
                        for link in status.retweeted_status.entities["urls"]:
                            expandedurl = link["expanded_url"]
                            sqllink = """SELECT link_id FROM Links WHERE url = '%s'""" % \
                                (str(expandedurl))
                            cursor.execute(sqllink)
                            myresultlink = cursor.fetchone()
                            if myresultlink == None :
                                urlsplit = expandedurl.split("//", 1)
                                domainlink = urlsplit[1]
                                domainsplit = domainlink.split("/", 1)
                                domain = domainsplit[0]
                                try:
                                    insertvaluetwitterlinks(expandedurl, domain,cursor,connection)
                                    sqllinkid = """SELECT link_id FROM Links WHERE url = '%s'""" % \
                                            (str(expandedurl))
                                    cursor.execute(sqllinkid)
                                    myresultlinkid = cursor.fetchone()
                                    insertvaluetwitterlinks2tweet(status.id, myresultlinkid[0],cursor,connection)
                                except:
                                    print("To link den mpike.\n")
                            else:
                                insertvaluetwitterlinks2tweet(status.id, myresultlink[0],cursor,connection)
                    if 'media' in status.retweeted_status.entities:
                        for image in status.retweeted_status.entities["media"]:
                            mediaurl = (image["media_url_https"])
                            sqlmedia = """SELECT media_id FROM Media WHERE media_text = '%s'""" % \
                                    (str(mediaurl))
                            cursor.execute(sqlmedia)
                            myresultmedia = cursor.fetchone()
                            if myresultmedia == None :
                                try:
                                    insertvaluetwittermedia(mediaurl,cursor,connection)
                                    sqlmediaid = """SELECT media_id FROM Media WHERE media_text = '%s'""" % \
                                            (str(mediaurl))
                                    cursor.execute(sqlmediaid)
                                    myresultmediaid = cursor.fetchone()
                                    insertvaluetwittermedia2tweet(status.id,myresultmediaid[0],cursor,connection)
                                except:
                                    print("To media den mpike.\n")
                            else:
                                insertvaluetwittermedia2tweet(status.id, myresultmedia[0],cursor,connection)
                    if 'hashtags' in status.retweeted_status.entities:
                        for hashtag in status.retweeted_status.entities["hashtags"]:
                            text = hashtag["text"]
                            sqlhashtag = """SELECT hash_id FROM Hashtag WHERE value = '%s'""" % \
                                    (str(text))
                            cursor.execute(sqlhashtag)
                            myresulthashtag = cursor.fetchone()
                            if myresulthashtag == None:
                                try:
                                    insertvaluetwitterhashtag(text,cursor,connection)
                                    sqlhashtagid = """SELECT hash_id FROM Hashtag WHERE value = '%s'""" % \
                                             (str(text))
                                    cursor.execute(sqlhashtagid)
                                    myresulthashtagid = cursor.fetchone()
                                    insertvaluetwitterhash2tweet(status.id, myresulthashtagid[0],cursor,connection)
                                except:
                                    print("To hashtag den mpike.\n")
                            else:
                                insertvaluetwitterhash2tweet(status.id,myresulthashtag[0],cursor,connection)
                except:
                    print("to tweet den mpike\n")

            else:
                insertvaluetwitterposts(status.id, status.user.id, status.retweet_count, status.favorite_count,
                                        status.created_at, status.full_text,cursor,connection)
                if 'user_mentions' in status.entities:
                    for usermention in status.entities["user_mentions"]:
                        mention = usermention["screen_name"]
                        insertvaluetwittermentions(status.id, mention,cursor,connection)
                if 'urls' in status.entities:
                    for link in status.entities["urls"]:
                        expandedurl = link["expanded_url"]
                        b=cleantextfromquotes(expandedurl)
                        sqllink = """SELECT link_id FROM Links WHERE url = '%s'""" % \
                                  (b)
                        cursor.execute(sqllink)
                        myresultlink = cursor.fetchone()
                        if myresultlink == None:
                            try:
                                urlsplit = expandedurl.split("//", 1)
                                domainlink = urlsplit[1]
                                domainsplit = domainlink.split("/", 1)
                                domain = domainsplit[0]
                                insertvaluetwitterlinks(expandedurl, domain,cursor,connection)
                                sqllinkid = """SELECT link_id FROM Links WHERE url = '%s'""" % \
                                            (str(expandedurl))
                                cursor.execute(sqllinkid)
                                myresultlinkid = cursor.fetchone()
                                insertvaluetwitterlinks2tweet(status.id, myresultlinkid[0],cursor,connection)
                        else:
                            insertvaluetwitterlinks2tweet(status.id, myresultlink[0],cursor,connection)
                if 'media' in status.entities:
                    for image in status.entities["media"]:
                        mediaurl = (image["media_url_https"])
                        sqlmedia = """SELECT media_id FROM Media WHERE media_text = '%s'""" % \
                                   (str(mediaurl))
                        cursor.execute(sqlmedia)
                        myresultmedia = cursor.fetchone()
                        if myresultmedia == None:
                            try:
                                insertvaluetwittermedia(mediaurl,cursor,connection)
                                sqlmediaid = """SELECT media_id FROM Media WHERE media_text = '%s'""" % \
                                            (str(mediaurl))
                                cursor.execute(sqlmediaid)
                                myresultmediaid = cursor.fetchone()
                                insertvaluetwittermedia2tweet(status.id, myresultmediaid[0],cursor,connection)
                            except:
                        else:
                            insertvaluetwittermedia2tweet(status.id, myresultmedia[0],cursor,connection)
                if 'hashtags' in status.entities:
                    for hashtag in status.entities["hashtags"]:
                        text = hashtag["text"]
                        sqlhashtag = """SELECT hash_id FROM Hashtag WHERE value = '%s'""" % \
                                     (str(text))
                        cursor.execute(sqlhashtag)
                        myresulthashtag = cursor.fetchone()
                        if myresulthashtag == None:
                            try:
                                insertvaluetwitterhashtag(text,cursor,connection)
                                sqlhashtagid = """SELECT hash_id FROM Hashtag WHERE value = '%s'""" % \
                                            (str(text))
                                cursor.execute(sqlhashtagid)
                                myresulthashtagid = cursor.fetchone()
                                insertvaluetwitterhash2tweet(status.id, myresulthashtagid[0],cursor,connection)
                            except:

                        else:
                            insertvaluetwitterhash2tweet(status.id, myresulthashtag[0],cursor,connection)
        else:
            updatevaluetwitterposts(status.id, status.retweet_count, status.favorite_count,cursor,connection)



def insertvaluetwitterusers(user_id, twitter_user, followers_count, following_count, statuses_count, favourites_count, description,cursor,connection):
  d=deEmojify(description)
  sql = """INSERT INTO TWITTER (twitter_user_id, twitter_username, followers, following, tweets, favorites, description) 
	VALUES ('%d', '%s', '%d', '%d', '%d', '%d','%s')""" % \
	(int(user_id), str(twitter_user), int(followers_count), int(following_count), int(statuses_count), int(favourites_count), d)
  try:
      cursor.execute(sql)
      connection.commit()

  except:
      connection.rollback()


def updatevaluetwitterusers(user_id ,twitter_user, followers_count, following_count, statuses_count, favourites_count, description,cursor,connection):
    d = deEmojify(description)
    sql3 = """UPDATE TWITTER SET twitter_username = '%s' ,followers = '%d', following = '%d', tweets = '%d', favorites = '%d', description = '%s' WHERE twitter_user_id = '%d' 
    """ % \
          (str(twitter_user), int(followers_count), int(following_count), int(statuses_count),
           int(favourites_count), d, int(user_id))
    try:
        cursor.execute(sql3)
        connection.commit()

    except:
        connection.rollback()


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


def insertvaluetwitterposts(tweet_id, user_id, retweet_count, favorite_count, created_at, plaintext,cursor,connection):
  tweetid_as_int64 = numpy.int64(tweet_id)
  a=cleantextfromquotes(plaintext)
  sql1 = """INSERT INTO TWEETS (tweet_id, createdat, twitter_user_id, favorite, retweet_count, tweet_plaintext) 
	VALUES ('%d', '%s', '%s', '%d', '%d', '%s')""" % \
    (tweetid_as_int64, str(created_at), int(user_id), int(favorite_count), int(retweet_count),str(a))
  try:
      cursor.execute(sql1)
      connection.commit()

  except:
      connection.rollback()


def updatevaluetwitterposts(tweet_id, retweet_count, favorite_count,cursor,connection):
    tweetid_as_int64 = numpy.int64(tweet_id)
    sql4 = """UPDATE TWEETS SET retweet_count = '%d' ,favorite = '%d' WHERE tweet_id = '%d'
    	""" % \
           (int(retweet_count), int(favorite_count), tweetid_as_int64)
    try:
        cursor.execute(sql4)
        connection.commit()
    except:
        connection.rollback()


def inserttwitteruserid(alias,user_id,cursor,connection):
    val=(alias, user_id, None , None)
    sql = "INSERT INTO USER (alias, twitter_user_id, facebook_user_id, instagram_user_id) VALUES (%s, %s, %s, %s)"

    try:
        cursor.execute(sql,val)
        connection.commit()

    except:
        connection.rollback()



def insertvaluetwittermentions(tweet_id, mention,cursor,connection):
  tweetid_as_int64 = numpy.int64(tweet_id)
  sql = """INSERT INTO TwitterUserMentions (tweet_id,mention_name) \
	VALUES ('%d', '%s')""" % \
	(tweetid_as_int64, str(mention))
  try:
      cursor.execute(sql)
      connection.commit()
  except:
      connection.rollback()


def insertvaluetwitterlinks(url , domain,cursor,connection):
  sql = """INSERT INTO Links (url,domain) \
	VALUES ('%s', '%s')""" % \
	(str(url), str(domain))
  try:
      cursor.execute(sql)
      connection.commit()
  except:
      connection.rollback()


def insertvaluetwittermedia(media_text,cursor,connection):
  sql = """INSERT INTO Media (media_text) \
	VALUES ('%s')""" % \
	(str(media_text))
  try:
      cursor.execute(sql)
      connection.commit()
  except:
      connection.rollback()


def insertvaluetwitterhashtag(value,cursor,connection):
  sql = """INSERT INTO Hashtag (value) \
	VALUES ('%s')""" % \
	(str(value))
  try:
      cursor.execute(sql)
      connection.commit()
  except:
      connection.rollback()


def insertvaluetwitterlinks2tweet(tweet_id,link_id,cursor,connection):
  sql = """INSERT INTO Links2tweet (tweet_id,link_id) \
	VALUES ('%d','%d')""" % \
	(int(tweet_id),int(link_id))
  try:
      cursor.execute(sql)
      connection.commit()
  except:
      connection.rollback()


def insertvaluetwittermedia2tweet(tweet_id,media_id,cursor,connection):
  sql = """INSERT INTO Media2tweet (tweet_id,media_id) \
	VALUES ('%d','%d')""" % \
	(int(tweet_id),int(media_id))
  try:
      cursor.execute(sql)
      connection.commit()
  except:
      connection.rollback()


def insertvaluetwitterhash2tweet(tweet_id,hash_id,cursor,connection):
  sql = """INSERT INTO Hash2tweet (tweet_id,hash_id) \
	VALUES ('%d','%d')""" % \
	(int(tweet_id),int(hash_id))
  try:
      cursor.execute(sql)
      connection.commit()
  except:
      connection.rollback()





