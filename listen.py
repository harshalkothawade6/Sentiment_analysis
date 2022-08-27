import tweepy
from tweepy import StreamingClient, OAuth1UserHandler
import socket

consumer_key='Ke6UVssnGYhTwsZ2MPPEYMHTL'
consumer_secret='oBrtZ4BV4vwTE3ozxCAZ2KfgXhJBBo71wLf8rsppgDYnGjTJtA'
broker_token='AAAAAAAAAAAAAAAAAAAAABhFYgEAAAAA7K0ZN3J0HoNhbUCPPCMN9bLgylg%3DS3wgyX9FuQplDj4rMCs2pdAqacYxwZg3m6YQG0537X518eXM1t'
access_token ='796384762956873728-1R4exOJHYDYfCVOiHEmqvTH5GaN5IoO'
access_secret='WImwP6hrZglrcwQir77brrRYygSTHoQCfgrpJOAS01Yk1'

# authentication based on the credentials

client = tweepy.Client(broker_token, consumer_key, consumer_secret, access_token, access_secret)
auth = OAuth1UserHandler(consumer_key, consumer_secret, access_token, access_secret)
api = tweepy.API(auth)

class MyStream(tweepy.StreamingClient):
    def __init__(self,csocket, broker_token, api):
        super().__init__(broker_token)
        self.client_socket = csocket

    # This function gets called when the stream is working
    def on_connect(self):

        print("Connected")


    # This function gets called when a tweet passes the stream
    def on_tweet(self, data):
        try:
            msg = data
            print("new message")
            # if tweet is longer than 140 characters
            if "extended_tweet" in msg:
                # add at the end of each tweet "t_end"
                self.client_socket \
                    .send(str(msg['extended_tweet']['full_text'] + "t_end") \
                          .encode('utf-8'))
                print(msg['extended_tweet']['full_text'])
            else:
                # add at the end of each tweet "t_end"
                self.client_socket \
                    .send(str(msg['text'] + "t_end") \
                          .encode('utf-8'))
                print(msg['text'])
            return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))
        return True

    def on_error(self, status):
        print(status)
        return True

        """  # Displaying tweet in console
        if tweet.referenced_tweets == None:
            print(tweet.text)
            client.like(tweet.id)
        """
def sendData(c_socket, keyword):
    print('start sending data from Twitter to socket')
    stream = MyStream(c_socket,broker_token,api)
    # start sending data from the Streaming API
    stream.add_rules(tweepy.StreamRule(keyword))
    print(keyword)
    stream.filter(tweet_fields=["referenced_tweets"])

if __name__ == "__main__":
    # server (local machine) creates listening socket
    s = socket.socket()
    host = "0.0.0.0"
    port = 5555
    s.bind((host, port))
    print('socket is ready')
    # server (local machine) listens for connections
    s.listen(4)
    print('socket is listening')
    # return the socket and the address on the other side of the connection (client side)
    c_socket, addr = s.accept()
    print("Received request from: " + str(addr))
    # select here the keyword for the tweet data
    sendData(c_socket, keyword = 'piano')