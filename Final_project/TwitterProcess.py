"""
Run process using 'python TwitterProcess.py'
"""
import TwitterProcessModule as tpmod
import datetime as dt

def start_database():
    #Connecting to sqlite3 database
    dbConn = tpmod.Sqlite3Db('social_network.db')

    #Creating tweet Dates table
    dbConn.query('''CREATE TABLE IF NOT EXISTS TweetDates(firstdate timestamp, lastdate timestamp)''')

    #Creating tweets table
    dbConn.query('''CREATE TABLE IF NOT EXISTS Tweets(insert_timestamp timestamp, tweet_timestamp timestamp, tweet_term TEXT, tweet TEXT, place_type TEXT, place_name TEXT, place_full_name TEXT, place_country_code TEXT, place_country TEXT)''')

    return dbConn

def twitter_authentication():
    twitterAuthentication = tpmod.TwitterAuthentication(version = "1.0")
    api = twitterAuthentication.connect()

    return api

def download_tweets(api, dbConn):
    twitterProcess = tpmod.TwitterProcess(api = api, dbConn = dbConn)
    limit, remaining, next_reset_time = twitterProcess.get_api_limits()
    print(limit, remaining, next_reset_time)

    #Exit when we cannot continue
    if remaining == 0:
        quit()
    
    dbConn.query('''SELECT count(1) cant FROM TweetDates; ''')
    sql_stm_res = dbConn.cursor.fetchall()

    sqlite3DbHelper = tpmod.Sqlite3DbHelper(dbConn = dbConn)
    query_stm_res = sqlite3DbHelper.get_count_stm(sql_stm_res)
    query_stm_res

    firstdate, lastdate = sqlite3DbHelper.get_process_dates(query_stm_res)
    noOfSearch = 10
    print(firstdate)
    print(lastdate)
    print(noOfSearch)
    
    twitterProcess.ins_twitter_tweets_loop(firstdate = firstdate, lastdate = lastdate, noOfSearch = noOfSearch)
    dbConn.close()


def main():
    """Execute routines"""
    print(f"Started new execution at: {dt.datetime.now()}")
    dbConn = start_database()
    api = twitter_authentication()
    download_tweets(api = api, dbConn = dbConn)
    print(f"Terminated new execution at: {dt.datetime.now()}")

if __name__ == "__main__":
    main()
