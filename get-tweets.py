#!/usr/bin/python
""" usage: get-tweets query [max_pages]
    query       required query to get data
    max_pages   the maximum number of historical pages
                the default is %(max_pages)d pages
                max is 50.
"""
import os
import urllib2
import simplejson
import cgi
import codecs
import datetime
import yaml
from sys import argv
from time import sleep
from operator import itemgetter

twitter_search ='http://search.twitter.com/search'
max_pages    = 50
max_attempts = 10
twitter_get  = {
    'rpp'           : 100,
    'format'        : 'json',
    'geocode'       : '',
}
primary_key = 'id'
geos = {
    'type'      : None,
    'latitude'  : None,
    'longitude' : None,
}
ignores = ['metadata', 'geo']
encoding = 'utf-8'
delim = '|'

# date time from created_at
created_at = [u'created_at_datetime', u'created_at_datetime_shift']
created_at_format = '%a, %d %b %Y %H:%M:%S'
created_at_format_out = '%m/%d/%Y %H:%M:%S'


#
# main()
#
def main(query, max_pages):
    print 'Starting: %s' % query
    # create the output file
    fn = query.replace(' ', '_').replace('"','').replace("'","") + '.csv'

    # check to see if the file exists, if so get the last twitter ID
    get_dict = {}
    keys = []
    tweet_ids = []
    if os.path.isfile(fn):
        tweet_ids, keys = get_last_id(fn, primary_key, delim)
        last_id = tweet_ids[-1]
        print 'Found the csv file, getting last id %d' % last_id
        get_dict['since_id'] = last_id

    # setup the search string
    get_dict['q'] = query
    search = get(get_dict, twitter_get)

    # get the twitters
    print 'Getting Tweets for "%s"' % query
    t = get_tweets(search)

    # if nothing, then we do not continue
    if len(t) == 0:
        print 'Unable to get any tweets'
        exit(2)

    # get the results
    tweets  = results(t)

    # check length
    if len(tweets) < 1:
        print "No tweets found for %s" % query
        exit(1)

    # get the historical data
    if max_pages > 0:
        for p in xrange(max_pages):
            if not t.has_key('next_page'):
                print 'No more pages found.'
                break
            next = t['next_page']
            print 'Getting Tweets for page %d' % (p+1)
            get_dict = get_to_dict(next)
            search   = get(get_dict, twitter_get)
            t        = get_tweets(search)
            tweets += results(t)

    # clean the tweets
    print "Cleaning the data"
    tweets, keys  = clean_tweets(tweets, keys)

    # make sure that we have all of the keys
    for c in created_at:
        if c in keys: continue
        keys.append(c)
    keys = arrange_keys(keys)

    # make sure all duplicates are removed
    tweets = remove_duplicates(tweets, tweet_ids)

    # make sure that the datetimes are setup correctly
    tweets = add_datetimes(tweets)

    # write out
    print "Writing %d tweets to CSV file %s" % (len(tweets), fn)
    write_to_csv(fn, tweets, keys)

# search_url:
#   setup the serach string
def get(get_dict, twitter_gets={}):
    q = get_dict['q']
    del get_dict['q']
    s = 'q=%s&' % html_encode(q)
    g = []
    # add the twitter gets to get
    for k,v in twitter_gets.items():
        if get_dict.has_key(k): continue
        get_dict[k] = v

    # setup for the url string
    for k,v in get_dict.items():
        g.append('%s=%s' % (k, str(v)))

    # create the url string
    g = '&'.join(g)
    s += g
    return twitter_search + '?' + s

# get_to_dict:
#   change the GET to a dictionary
def get_to_dict(get, original_get={}):
    get = get.split('?')[-1]
    out = {}
    for l in get.split('&'):
        l = l.split('=')
        k = l[0]
        if len(l) < 2: print l
        out[l[0]] = l[1]
    for k,v in original_get.items():
        # assume we do not want to change it if it does not exist
        if k in out.keys(): continue
        out[k] = v
    return out

# get_tweets:
#   grab all twitter results (100) from the twitter search
def get_tweets(search):
    tweets = {}
    attempts = 0
    while True:
        if attempts == max_attempts: break
        try:
            # since we are just dealing with json
            # just load the json
            tweets = simplejson.load(
                urllib2.urlopen(search)
                )
        except simplejson.decoder.JSONDecodeError, jerr:
            # just return the empty dict.
            pass
        except urllib2.HTTPError, herr:
            # access denied
            attempts += 1
            print 'Access Denied %d, attempting in 10 seconds' % attempts
            sleep(10)
            continue
        except Exception, err:
            raise err
        break
    return tweets

# results:
#   return the results
def results(tweets):
    if not tweets.has_key('results'):
        raise Exception('no results existed for tweets')
    return tweets['results']

# keys:
#   get the first row of keys, sort and and set
#   primary key on the first column
def arrange_keys(keys, pkey=primary_key):
    # sort the keys
    keys.sort()
    # remove the primary key and insert into the
    # keys as the first column
    keys.remove(pkey)
    keys.insert(0, pkey)
    return keys

# clean_tweets:
#   clean the tweet data
def clean_tweets(tweets, keys=[]):
    res = []
    for r in tweets:
        # add the geo's extra columns to the data
        for k,v in geos.items(): r[unicode(k)] = v
        r = add_geo(r)
        for i in ignores: del r[i]
        for k in r.keys():
            if not k in keys: keys.append(k)
        res.append(r)
    # sort the rows
    res = sorted(res, key=itemgetter('id'))
    return res, keys

# clean_val:
#   clean the values, if they are strings
def clean_val(v):
    replaces = [
        ('\n' , ''),
        ('\r' , ''),
        (delim, '-')
        ]
    if not type(v) is unicode:
        if v == None: v = ''
        return unicode(v)
    v = unicode(v).strip()
    for r in replaces:
        v = v.replace(r[0], r[1])
    return v

# add_geo:
#   split the geos and add the values if they exists
def add_geo(r):
    geo = r['geo']
    if geo == None: return r
    r['type']      = geo['type']
    r['latitude']  = geo['coordinates'][0]
    r['longitude'] = geo['coordinates'][1]
    return r

# html_encode:
#   encode the string into html
def html_encode(s):
    """ encode to HTML values """
    encodes = {
        " " : "%20",
        "#" : "%23",
        '"' : "%22",
        "'" : "%27",
        "@" : "%40"
        }
    _s = ''
    for c in s:
        if encodes.has_key(c):
            _s += encodes[c]
        else:
            _s += c
    _s = cgi.escape(_s)
    return _s

# write_to_csv:
#   write the results to the csv file
def write_to_csv(fn, results, keys, delim=delim):
    mode = 'wb'
    add_header = True
    if os.path.isfile(fn):
        # make sure that we have the correct keys
        add_header = False
        mode = 'ab'
    f = codecs.open(fn, mode, encoding=encoding)
    if add_header:
        f.write(delim.join(keys) + '\n')
    for i, r in enumerate(results):
        line = []
        for k in keys:
            v = None
            if r.has_key(k): v = r[k]
            v = clean_val(v)
            line.append(v)
        line = delim.join(line)
        if line.strip() == '': continue
        f.write(line)
        if i < len(results)-1: f.write('\n')
    f.close()
    return fn

# get_last_id
#   get the last id from the csv file
def get_last_id(fn, primary_key=primary_key, delim=delim):
    if not os.path.isfile(fn): return None
    f = codecs.open(fn, 'rb', encoding=encoding)
    lines = f.readlines()
    f.close()

    # populate the tweet ids
    header = lines.pop(0).strip()
    keys    = header.split(delim)

    # clean and sort the lines (making sure that we do not have duplicate lines)
    lines = list(set(lines))
    lines = sorted(lines, key=lambda s: s.split(delim)[0])
    lines = [l.strip() for l in lines if l.strip() != '']

    # get just the tweet ids
    tweet_ids = [l.split(delim)[0] for l in lines if not l.strip() == '']

    # just double check that the list is sorted, is really overkill.
    tweet_ids.sort()

    # make sure tweet_ids are ints
    _tweet_ids = []
    for t in tweet_ids:
        try:
            t = int(t)
        except:
            continue
        _tweet_ids.append(t)
    tweet_ids = _tweet_ids
    # add the keys back to the lines
    lines = [l.strip().split(delim) for l in lines]
    lines.insert(0, keys)

    # make sure that previous tweets have the datetimes added
    lines = add_datetimes(lines)

    # since we are alread in here, just write the csv file as a sorted csv file
    f = codecs.open(fn, mode='wb', encoding=encoding)
    f.write(header + '\n')
    lh = len(header.split(delim))
    for i,l in enumerate(lines):
        l = delim.join(l)
        if l.strip() == '': continue
        ls = l.split(delim)
        if len(ls) > lh: continue
        f.write(l)
        if i < len(lines)-1: f.write('\n')
    f.close()

    # return tweet ids and keys
    return tweet_ids, keys

# add_datetimes:
def add_datetimes(items):
    if type(items[0]) is list:
        items = add_datetimes_list(items)
    else:
        items = add_datetimes_dict(items)
    return items

def add_datetimes_dict(items):
    out = []
    for line in items:
        ca = line['created_at']
        dt, dt_shift = convert_time(ca)
        line[created_at[0]] = dt
        line[created_at[1]] = dt_shift
        out.append(line)
    return out

# add_datetimes_list:
def add_datetimes_list(lines):
    header = lines.pop(0)

    # add extra to the headers
    for e in created_at:
        if e in header: continue
        header.append(e)
    # add the created_at datetimes to header
    header = sort_keys(header)

    # fix the lines
    k = header.index('created_at')
    out = []
    for line in lines:
        if len(line) == len(header):
            out.append(line)
            continue
        ca = line[k]
        if ca == 'created_at': continue
        dt, dt_shift = convert_time(ca)

        # add to the line
        dt_i = header.index(created_at[0])
        dt_shift_i = header.index(created_at[1])
        line.insert(dt_i, dt)
        line.insert(dt_shift_i, dt_shift)
        out.append(line)
    # readd the header
    out.insert(0, header)
    return out

# convert_time:
def convert_time(ca):
    # split on +/- to get GMT shift
    if ca.find('+') > -1:
        dt, dt_shift = ca.split("+")
        dt_shift = '+' + dt_shift
    else:
        dt, dt_shift = ca.split("-")
        dt_shift = '-' + dt_shift

    # clean the strings
    dt = dt.strip()
    dt_shift = dt_shift.strip()

    # clean the strings
    dt = dt.strip()
    dt_shift = dt_shift.strip()

    # turn into datetime object
    dt = datetime.datetime.strptime(dt, created_at_format)

    # turn the datetime object to a string
    dt = dt.strftime(created_at_format_out)

    return dt, dt_shift

# sort_keys:
#   sort the keys, making sure that they are always in the same order
def sort_keys(keys, primary='id'):
    keys.sort()
    p = keys.pop(keys.index(primary))
    keys.insert(0, p)
    return keys

# remove_duplicates
#   make sure that we are not publishing duplicates to the
#   the csv file
def remove_duplicates(tweets, ids, primary_key=primary_key):
    # make sure we are dealing with integers
    ids = [int(i) for i in ids if not str(i).strip() == '' ]
    print "There are currently '%d' tweets" % len(tweets)
    tweets_out = []
    for tweet in tweets:
        if len(tweet) < 1: continue
        id = int(tweet[primary_key])
        if id in ids: continue
        tweets_out.append(tweet)
    print "Adding '%d' tweets to the CSV file" % len(tweets_out)
    return tweets_out


# usage
def usage(args, max_pages):
    if len(args) == 0:
        print __doc__ % vars()
        exit(2)
    query = args[0].lower().strip()
    if query == '':
        print __doc__ % vars()
        print "Query string was empty, do nothing"
        exit(2)

    if len(args) > 1:
        max_pages = int(args[1])
    return query, max_pages

if __name__ == '__main__':
    args = argv[1:]
    if len(args) == 0:
        print __doc__
        exit(1)

    q = args.pop(0)
    m = max_pages 
    if len(args) > 0: m = int(args.pop(0))
    main(q,m)
