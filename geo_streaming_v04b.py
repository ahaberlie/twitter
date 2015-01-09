# https://dev.twitter.com/streaming/overview/request-parameters

import twitter
import time
import os

# https://dev.twitter.com/apps/new
ckey = key
csec = key
atok = key
asec = key

t = twitter.Api(consumer_key=ckey,consumer_secret=csec,access_token_key=atok,access_token_secret=asec)
print t.VerifyCredentials()

#%%
# Bounding Box Tuples.  SW corner first, then NE (lon,lat)
# Bounding boxes from http://isithackday.com/geoplanet-explorer/index.php?woeid=2347574
# bounding_box = [-96.639427,40.375469,-90.140350,43.501030] # Iowa
# bounding_box = [-104.057693,35.904160,-80.518700,49.385311] # Midwest
bounding_box = [-180,-90,180,90]
#bounding_box = [-125.001106, 24.949320, -66.932640, 49.590370] # CONUS

# Convert to list of strings

bounding_box = [str(i) for i in bounding_box]

# Attempt to repair stream empty faults
num_stream_empty = 10

# Describe headerline
header_row = "time \t tweet_id \t user_id \t lat \t lon \t user_location \t tweet_text\n"

# Get current GMT, and use it to open a file
now = time.gmtime()
filename_format = "%Y%m%d"
filename = time.strftime(filename_format,now) + '.txt'

# See if file exists; if it does, we won't write the header
already_exists = os.path.isfile(filename)

f = open(filename,'a')
if not already_exists:
    f.write(header_row)

# Open a stream
stream = t.GetStreamFilter(locations=bounding_box)

while True:
#for i in range(50):  # Just for testing short bursts
    try:
        # Grab a new tweet
        this_tweet = stream.next()
        print this_tweet["text"].encode('ascii','ignore')
        
        # Check to see if the filename needs to change (new GMT day)
        then = now
        now = time.gmtime()
        if (now.tm_mday) != (then.tm_mday):
        #if (now.tm_hour) != (then.tm_hour):
            print "OPENING NEW FILE."
            f.close()
            filename = time.strftime(filename_format,now) + '.txt'
            f = open(filename,'a')
            f.write(header_row)
        
        # And continue writing the data
        try:
            # gmt, milliseconds since last epoch
            f.write("{:.3f}\t".format(float(this_tweet["timestamp_ms"])/1000))
            
            # tweet_id
            f.write(format(this_tweet["id"]))
            f.write("\t")
            # user_id
            f.write(format(this_tweet["user"]["id"]))
            f.write("\t")

            # lat/lon
            if this_tweet["geo"]!=None:
                f.write(format(this_tweet["geo"]["coordinates"][0]))
                f.write("\t")
                f.write(format(this_tweet["geo"]["coordinates"][1]))
                f.write("\t")
            else:
                f.write("\t\t")

            # user_location; tabs and newline chracters are removed
            if this_tweet["user"]["location"]!=None:
                f.write(this_tweet["user"]["location"].encode('utf-8','ignore').replace("\r"," ").replace("\n"," ").replace("\t"," "))
                f.write("\t")
            else:
                f.write("\t")

            # tweet_text; tabs and newline characters are removed
            f.write(format(this_tweet["text"].encode('utf-8','ignore').replace("\r"," ").replace("\n"," ").replace("\t"," ")))

            # Terminate with newline character
            f.write("\n")
            
            num_stream_empty = 0
        except:
            print "ERROR WRITING TO FILE"  
    except:
        print "STREAM EMPTY"
        time.sleep(1)
        num_stream_empty += 1
        if num_stream_empty >= 10:
            stream = t.GetStreamFilter(locations=bounding_box)
            num_stream_empty = 0
            
            

stream.close()
f.close()
