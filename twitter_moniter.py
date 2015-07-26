
import twitter
from twitter import TwitterError
from config import *
import datetime
import logging
from logging.handlers import SMTPHandler
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Integer, String
from sqlalchemy import MetaData
from sqlalchemy.dialects.mysql import LONGTEXT, MEDIUMTEXT
from httplib import IncompleteRead

Base = declarative_base()
metadata = MetaData()

class Tweet(Base):
    
    __tablename__ = 'Tweets'
    
    metadata = metadata
    id = Column(Integer, primary_key=True)
    created_at = Column(String(100), default=None)
    contributors = Column(LONGTEXT, default=None)
    favorite_count = Column(String(100), default=None)
    id_str = Column(String(100), default=None)
    lang = Column(String(40), default=None)
    retweet_count = Column(String(40), default=None)
    coordinates = Column(MEDIUMTEXT, default=None)
    in_reply_to_status_id_str = Column(String(100), default=None)
    in_reply_to_screen_name = Column(String(100), default=None)
    in_reply_to_user_id_str = Column(String(100), default=None)
    place = Column(MEDIUMTEXT, default=None)
    quoted_status_id_str = Column(String(100), default=None)
    retweeted_status = Column(MEDIUMTEXT, default=None)
    source = Column(MEDIUMTEXT, default=None)
    text = Column(String(140), default=None)
    user = Column(LONGTEXT, default=None)
    withheld_copyright = Column(MEDIUMTEXT, default=None)
    withheld_in_countries = Column(MEDIUMTEXT, default=None)
    
class StreamError(Exception):
    @property
    def message(self):
        return self.args[0]
            
def record(captured, session):
    '''
    Record a batch of captured tweets using given sqlalchemy session
    '''
    
#    tweets = []
    
    for t in captured:
        tweet = Tweet()
        for key in t:
            if hasattr(tweet, key) and t[key] and key != 'id':
                setattr(tweet, key, unicode(t[key]))
#        print "Tweet \n"
#        print tweet.id
#        print tweet.text
#        print tweet.id_str
#        print tweet.__dict__
        #tweets.append(tweet)
        session.add(tweet)
        session.commit()
    
#    session.add_all(tweets)
#    session.commit()
    
def connect(api, **kwargs):
    '''
    Connect to a stream; use exponential or linear backoff
    '''
    timer = 0
    attempts = 0
    
    logger = kwargs.get('logger')
    max_attempts = kwargs.get('max_attempts', 10)
    backoff = kwargs.get('backoff', 'exponential')
    
    while True:
        
        try:
            return api.GetStreamFilter(**kwargs)
        
        except TwitterError as e:
            
            if backoff == 'linear':
                time.sleep(timer)
                timer += 1
                attempts += 1
                
            elif backoff == 'exponential':
                time.sleep(pow(timer, 2))
                timer += 1
                attempts += 1
                
            if attempts >= max_attempts:
                logger.exception("Max Connection Attempts Reached", e)
                raise StreamError("Max Connection Attempts Reached")
            

def moniter(api, batch_size, **kwargs):
    '''
    Drink from the twitter feed
    '''
    #filter stream parameters
    track = kwargs.get('track')
    delimited = kwargs.get('delimited')
    stall_warnings = kwargs.get('stall_warnings')
    follow = kwargs.get('follow')
    locations = kwargs.get('locations')
    
    #stream recording parameters
    max_capture = kwargs.get('max')
    stop_time = kwargs.get('stop_time')
    rate_limit = kwargs.get('rate_limit')
    batch_size = kwargs.get('batch_size', 20)
    session = kwargs.get('session')
    
    #logging
    logger = kwargs.get('logger')
    
    captured = []
    count = 0
    
    stream = connect(api, 
                     track=track, 
                     delimited=delimited,
                     stall_warnings=stall_warnings,
                     follow=follow,
                     locations=locations)
    
    while True:
        if stop_time and datetime.datetime.now() >= stop_time:
            if logger:
                logger.critical('Stop Time Reached')
            break
            
        if rate_limit:
            time.sleep(rate_limit)
        
        if max_capture and count > max_capture:
            if logger:
                logger.critical('Maximum Number of Tweets Captured')
            break
            
        if len(captured) >= batch_size:
            record(captured, session)
            count += batch_size
            del captured[:]
        
        try:
            tweet = stream.next()
        except (TwitterError, IncompleteRead) as e:
            message = e.message if type(e.message) == str else e.message['message']
            logger.warning(message)
            #might want to do more here later for individual errors
            try:
                stream = connect(api, 
                     track=track, 
                     delimited=delimited,
                     stall_warnings=stall_warnings,
                     follow=follow,
                     locations=locations)
                tweet = stream.next()
            except (StreamError, TwitterError) as e:
                logger.exception("Lost the Connection", e)
                raise StreamError("Lost the Connection")
            
        if type(tweet) != dict:
            #Keep alive signal recieved
            continue
        elif tweet.has_key('delete') or tweet.has_key('scrub_geo'):
            #Status deletion notice:
            #Not doing anything here yet, but should implement deletion protocols
            continue
        elif tweet.has_key('limit'):
            #Indicates that rate limit has been reached, and gives
            #information on how large the pool of unrecieved tweets is.
            #Not doing anything with this information yet.
            continue
        elif tweet.has_key('status_withheld') or tweet.has_key('user_withheld'):
            #Indicates that the tweet has had information witheld.
            #No reason to record or use this tweet.
            continue
        elif tweet.has_key('disconnect'):
            #Indicates that the stream has disconnected for some reason.
            #Handle errors or reconnection attempts accordingly.
            if tweet['disconnect']['code'] in [1,4,5,9,10,11,12]:
                #attempt to reconnect
                stream = connect(api, **kwargs)
            elif tweet['disconnect']['code'] in [2,6,7]:
                #critical error, alert user and fail
                logger.critical("Disconnected, error code: " + str(tweet['disconnect']['code']))
                raise StreamError("Disconnected, error code: " + str(tweet['disconnect']['code']))
            else:
                #unknown disconnect code, attempt to reconnect anyways
                stream = connect(api, **kwargs)
        elif tweet.has_key('event'):
            if tweet['event'] == 'user_update':
                continue
            elif tweet['event'] == 'access_revoked':
                logger.critical("Access Revoked")
                raise StreamError("Access Revoked")
        elif tweet.has_key('warning'):
            logger.warning(tweet['warning']['message'])
            continue
        else:
            captured.append(tweet)
            count += 1

if __name__ == '__main__':

    api = twitter.Api(consumer_key = CONSUMER_KEY,
                  consumer_secret = CONSUMER_SECRET,
                  access_token_key = ACCESS_TOKEN_KEY,
                  access_token_secret = ACCESS_TOKEN_SECRET)
                  
    engine = create_engine(ENGINE, encoding='utf-8')
    Session = sessionmaker(bind=engine)
    session = Session()
    metadata.create_all(engine)
    
    logging.basicConfig(filename='log.txt')
    logger = logging.getLogger(__name__)
    mail_handler = SMTPHandler('127.0.0.1', 
                               'logger@twitter_moniter.com',
                               EMAIL, 
                               'Critical Error')
    mail_handler.setLevel(logging.WARNING)
    logger.addHandler(mail_handler)
    
    moniter(api, engine=ENGINE, max_capture=MAX_CAPTURE, batch_size=BATCH_SIZE,
            track=TRACK, stall_warnings=True, logger=logger, session=session)
            
    session.close()
    
            