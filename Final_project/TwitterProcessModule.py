import pandas as pd
import time
import tweepy
import json
import csv
import sqlite3
import datetime as dt

class CloudAuthentication():
    """General cloud authentication"""
    def __init__(self, version):
        self.version = version
    
    def __str__(self):
        return f"General cloud authentication version {self.version}"
    
    def __get_credential(self, parameters):
        pass
    
    def connect(self, parameters):
        pass
    
    def status(self):
        pass

class TwitterAuthentication(CloudAuthentication):
    """Twitter authentication extended from CloudAuthentication"""
        
    def __get_credential(self, parameters):
        print(f"Path--> {parameters.at[0,'Path']}")
        print(f"File--> {parameters.at[0,'File']}")
        self.__twitter_auth_path_file = parameters.at[0,'Path'] + parameters.at[0,'File']
        self.__df_twitter_auth = pd.read_json(path_or_buf = self.__twitter_auth_path_file, orient='records')
        return self.__df_twitter_auth

    def __get_twitter_authentication_parameters(self):
        df_twitter_parameters = pd.DataFrame(columns = ['Path', 'File'])
        df_twitter_parameters = df_twitter_parameters.append({'Path' : "authentication/", 'File' : "twitter_auth.json"},  
                ignore_index = True)
        return df_twitter_parameters
              
    def connect(self):
        parameters = self.__get_twitter_authentication_parameters()
        self.__df_twitter_auth = self.__get_credential(parameters = parameters)
        #print(f"__df_twitter_auth--> {self.__df_twitter_auth}")
              
        #print(f"consumer_key--> {self.__df_twitter_auth.at[0,'consumer_key']}")
        #print(f"consumer_secret--> {self.__df_twitter_auth.at[0,'consumer_secret']}")
        #print(f"access_token--> {self.__df_twitter_auth.at[0,'access_token']}")
        #print(f"access_token_secret--> {self.__df_twitter_auth.at[0,'access_token_secret']}")
        
        self.__auth = tweepy.OAuthHandler(self.__df_twitter_auth.at[0,'consumer_key'], self.__df_twitter_auth.at[0,'consumer_secret'])
        self.__auth.set_access_token(self.__df_twitter_auth.at[0,'access_token'], self.__df_twitter_auth.at[0,'access_token_secret'])
        self.__api = tweepy.API(self.__auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
        return self.__api

class TwitterProcess:
    def __init__(self, api, dbConn):
        self.api = api
        self.dbConn = dbConn
    
    def get_tweets_hashtag(self, searchTerms):
        for searchTerm in searchTerms:
            print("The searchTerm is:{0}".format(searchTerm))

    def get_api_limits(self):
        limits = self.api.rate_limit_status()
        limit = limits['resources']['search']['/search/tweets']['limit']
        remaining = limits['resources']['search']['/search/tweets']['remaining']
        next_reset = limits['resources']['search']['/search/tweets']['reset']
        next_reset_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(limits['resources']['search']['/search/tweets']['reset']))
        return limit, remaining, next_reset_time
    
    def get_search_terms(self):
        #searchTerms = ["corona", "#corona", "coronavirus", "#coronavirus", "covid", "#covid", "covid19", "#covid19", "covid-19", "#covid-19", "sarscov2", "#sarscov2", "sars cov2", 
        #"sars cov 2", "covid_19", "#covid_19", "#ncov", "ncov", "#ncov2019", "ncov2019", "2019-ncov", "#2019-ncov", "pandemic", "#pandemic" "#2019ncov", "2019ncov",
        #"quarantine", "#quarantine", "flatten the curve", "flattening the curve", "#flatteningthecurve", "#flattenthecurve", "hand sanitizer", "#handsanitizer",
        #"#lockdown", "lockdown", "social distancing", "#socialdistancing", "work from home", "#workfromhome", "working from home", "#workingfromhome", "ppe", "n95",
        #"#ppe", "#n95", "#covidiots", "covidiots", "herd immunity", "#herdimmunity", "pneumonia", "#pneumonia", "chinese virus", "#chinesevirus", "wuhan virus",
        #"#wuhanvirus", "kung flu", "#kungflu", "wearamask", "#wearamask", "wear a mask", "vaccine", "vaccines", "#vaccine", "#vaccines", "corona vaccine",
        #"corona vaccines", "#coronavaccine", "#coronavaccines", "face shield", "#faceshield", "face shields", "#faceshields", "health worker", "#healthworker", 
        #"health workers", "#healthworkers", "#stayhomestaysafe", "#coronaupdate", "#frontlineheroes", "#coronawarriors", "#homeschool", "#homeschooling",
        #"#hometasking", "#masks4all", "#wfh", "wash ur hands", "wash your hands", "#washurhands", "#washyourhands", "#stayathome", "#stayhome", "#selfisolating",
        #"self isolating"]
        
        searchTerms = ["#corona", "#coronavirus", "#covid", "#covid19", "#covid-19", "#sarscov2", 
        "#covid_19", "#ncov", "#ncov2019", "#2019-ncov", "#pandemic", "#2019ncov",
        "#quarantine", "#flatteningthecurve", "#flattenthecurve", "#handsanitizer",
        "#lockdown", "#socialdistancing", "#workfromhome", "#workingfromhome",
        "#ppe", "#n95", "#covidiots", "#herdimmunity", "#pneumonia", "#chinesevirus",
        "#wuhanvirus", "#kungflu", "#wearamask", "#vaccine", "#vaccines",
        "#coronavaccine", "#coronavaccines", "#faceshield", "#faceshields", "#healthworker", 
        "#healthworkers", "#stayhomestaysafe", "#coronaupdate", "#frontlineheroes", "#coronawarriors", "#homeschool", "#homeschooling",
        "#hometasking", "#masks4all", "#wfh", "#washurhands", "#washyourhands", "#stayathome", "#stayhome", "#selfisolating"]        
        
        #searchTerms = ["#vaccination"]
        
        return searchTerms

    def ins_twitter_tweets(self, firstdate, lastdate, searchTerm, noOfSearch):
        #tweets = tweepy.Cursor(self.api.search , q=(searchTerm), 
        #                       lang="en",
        #                       since=firstdate,
        #                       until = lastdate).items(noOfSearch)
        
        #searchTerm = "#vaccination"
        #noOfSearch = 1
        
        #tweets = tweepy.Cursor(self.api.search , q=(searchTerm), 
        #               lang="en",
        #               since="2020-12-29",
        #               until = "2020-12-30").items(noOfSearch)

        #print(self.api)
        #print(searchTerm)
        #print(firstdate)
        #print(lastdate)
        #print(noOfSearch)
        #print(len(list(tweets)))
        #print("Outside")
        
        for tweet in tweepy.Cursor(self.api.search,q=(searchTerm),
                                   lang="en",
                                   since=firstdate,
                                   until=lastdate).items(noOfSearch):
        #for tweet in tweets:
            #print (tweet._json)
            #print("Inside")
            tweet_timestamp = tweet.created_at
            tweet_text = tweet.text.encode('utf-8')
            tweet_term = searchTerm.encode('utf-8')
            """
            print(tweet_timestamp)
            print(type(tweet_timestamp))
            print(tweet_text)
            print(type(tweet_text))
            """
            if tweet.place:
                tweet_place_type = tweet.place.place_type
                tweet_place_name = tweet.place.name
                tweet_place_full_name = tweet.place.full_name
                tweet_place_country_code = tweet.place.country_code
                tweet_place_country = tweet.place.country
                """
                print(tweet_place_type)
                print(type(tweet_place_type))

                print(tweet_place_name)
                print(type(tweet_place_name))

                print(tweet_place_full_name)
                print(type(tweet_place_full_name))

                print(tweet_place_country_code)
                print(type(tweet_place_country_code))

                print(tweet_place_country)
                print(type(tweet_place_country))
                """
            else:
                tweet_place_type = "Undefined"
                tweet_place_name = "Undefined"
                tweet_place_full_name = "Undefined"
                tweet_place_country_code = "Undefined"
                tweet_place_country = "Undefined"

            insert_timestamp = dt.datetime.now(dt.timezone.utc)
            #print("Here")
            print(tweet_text)
            ins_stm_str = """INSERT INTO Tweets (insert_timestamp, tweet_timestamp, tweet_term, tweet, place_type, place_name, place_full_name, place_country_code, place_country) VALUES (?,?,?,?,?,?,?,?,?);"""
            #print("Here2")
            data_tuple = (insert_timestamp, tweet_timestamp, tweet_term, tweet_text, tweet_place_type, tweet_place_name, tweet_place_full_name, tweet_place_country_code, tweet_place_country)
            #print("Here3")
            self.dbConn.query(sqlStm=ins_stm_str, sqlStmPrm=data_tuple)
            #print("Here4")
            #print(tweet_text)
            #self.dbConn.commit()
        
    def ins_twitter_tweets_loop(self, firstdate, lastdate, noOfSearch):
        start_date = firstdate.date()
        end_date = lastdate.date()
        delta = dt.timedelta(days=1)
        print("noOfSearch:" + str(noOfSearch))
        while start_date < end_date:
            dt_twitter_start = start_date
            dt_twitter_end   = start_date + (dt.timedelta(days=1))
            print(dt_twitter_start, "-", dt_twitter_end)
            
            #Search terms
            searchTerms = self.get_search_terms()
            for i, searchTerm in enumerate(searchTerms):
                #print (i, ",",searchTerm)
                self.ins_twitter_tweets(firstdate = dt_twitter_start, lastdate = dt_twitter_end, searchTerm = searchTerm, noOfSearch = noOfSearch)
            start_date += delta

#https://docs.python.org/3/library/sqlite3.html
class Sqlite3Db:
    """Sqlite3 Database General Methods"""
    def __init__(self, name=None):
        self.conn = None
        self.cursor = None

        if name:
            self.open(name)
    
    def open(self, name):
        try:
            self.conn = sqlite3.connect(name,detect_types=sqlite3.PARSE_DECLTYPES)
            self.cursor = self.conn.cursor()
        except sqlite3.Error as e:
            print("Error connecting to database " + name + " with message:" + e.args[0] )
    
    def close(self):
        if self.conn:
            self.conn.commit()
            self.cursor.close()
            self.conn.close()

    def query(self,sqlStm,sqlStmPrm=None):
        if sqlStmPrm is not None:
            #print("First")
            self.cursor.execute(sqlStm,sqlStmPrm)
        else:
            #print("Second")
            self.cursor.execute(sqlStm)

    #Using these magic methods (__enter__, __exit__) allows you to implement objects which can be used easily with the with statement.
    def __enter__(self):
        return self.conn
    
    def __exit__(self,exc_type,exc_value,traceback):
        self.close()

class Sqlite3DbHelper():
    """Sqlite3 Database Helper"""
    def __init__(self, dbConn):
        self.dbConn = dbConn
    def get_count_stm(self, sql_stm_res):
        res_list = [x[0] for x in sql_stm_res]
        return res_list[0]
    def get_timestamp_start_date(self):
        days_to_subtract = self.get_days_to_subtract()
        start_date = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=days_to_subtract)
        return start_date
    def get_timestamp_now(self):
        dateTimeObj = dt.datetime.now(dt.timezone.utc)
        return dateTimeObj
    def get_days_to_subtract(self):
        #Twitter restriction for this account type = 7 days
        days_to_subtract = 7
        return days_to_subtract
    def get_process_dates(self, query_stm_res):
        if query_stm_res == 0:
            print("one")
            dfTweetDates = pd.DataFrame(columns=['firstdate', 'lastdate'])
            firstdate = self.get_timestamp_start_date()
            lastdate = self.get_timestamp_now()
            ins_stm_str = """INSERT INTO TweetDates (firstdate,lastdate) VALUES (?,?);"""
            data_tuple = (firstdate, lastdate)
            self.dbConn.query(sqlStm=ins_stm_str, sqlStmPrm=data_tuple)
            sql_stm_res = self.dbConn.cursor.fetchall()
            if len(sql_stm_res) == 0:
                dfTweetDates['firstdate'] = firstdate
                dfTweetDates['lastdate']  = lastdate
            return firstdate, lastdate
        else:
            print("two")
            #Checking for tweet table
            self.dbConn.query('''SELECT count(1) cant FROM Tweets; ''')
            sql_stm_res = self.dbConn.cursor.fetchall()

            query_stm_res = self.get_count_stm(sql_stm_res)
            query_stm_res
            
            if query_stm_res == 0:
                dfTweetDates = pd.DataFrame(columns=['firstdate', 'lastdate'])
                firstdate = self.get_timestamp_start_date()
                lastdate = self.get_timestamp_now()
                ins_stm_str = """INSERT INTO TweetDates (firstdate,lastdate) VALUES (?,?);"""
                data_tuple = (firstdate, lastdate)
                self.dbConn.query(sqlStm=ins_stm_str, sqlStmPrm=data_tuple)
                sql_stm_res = self.dbConn.cursor.fetchall()
                if len(sql_stm_res) == 0:
                    dfTweetDates['firstdate'] = firstdate
                    dfTweetDates['lastdate']  = lastdate
                return firstdate, lastdate
        
                #firstdate = dt.datetime.min
                #lastdate  = dt.datetime.min
                #return firstdate, lastdate
            else:
                #Select rows from TweetDates
                sel_stm_str = '''SELECT firstdate, lastdate from TweetDates;'''
                self.dbConn.query(sel_stm_str)

                sqlStmRes = self.dbConn.cursor.fetchall()
                dfTweetDatesPrev = pd.DataFrame(sqlStmRes, columns =['firstdate', 'lastdate'])
                dfTweetDatesPrev = dfTweetDatesPrev[:1] 

                #print(dfTweetDatesPrev['firstdate'])
                #print(dfTweetDatesPrev['lastdate'])

                #Setting up dates
                firstdateTmp = pd.to_datetime(dfTweetDatesPrev['lastdate'].iloc[0])
                firstdate = firstdateTmp.to_pydatetime()
                lastdate  =  self.get_timestamp_now()

                #print(type(firstdate))
                #print(type(lastdate))

                #Cleaning TweetDates table
                del_stm_str = """DELETE FROM TweetDates;"""
                self.dbConn.query(sqlStm=del_stm_str)

                #Inserting TweetDates table
                ins_stm_str = """INSERT INTO TweetDates (firstdate,lastdate) VALUES (?,?);"""
                data_tuple = (firstdate, lastdate)
                self.dbConn.query(sqlStm=ins_stm_str, sqlStmPrm=data_tuple)

                #Select new rows from TweetDates
                sel_stm_str = '''SELECT firstdate, lastdate from TweetDates;'''
                self.dbConn.query(sel_stm_str)

                sql_stm_res = self.dbConn.cursor.fetchall()

                if len(sql_stm_res) == 0:
                    dfTweetDates = pd.DataFrame(sql_stm_res, columns =['firstdate', 'lastdate'])
                    dfTweetDates = dfTweetDatesPrev[:1]
                    dfTweetDates['firstdate'] = firstdate
                    dfTweetDates['lastdate']  = lastdate
                return firstdate, lastdate