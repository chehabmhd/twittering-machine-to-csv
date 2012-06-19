#!/usr/bin/python
"""
Read Twitter JSON dearta and convert to a CSV file
based on the QUERIES supplied in the QUERY_YAML
file.
"""
import os
import sys
import urllib2
import simplejson
import codecs
import yaml
import datetime
import re
import csv
import datetime

from time import mktime,sleep
from HTMLParser import HTMLParser
from rfc822 import parsedate_tz
from argparse import ArgumentParser,FileType
from urllib import urlencode, quote_plus, unquote_plus
from cgi import parse_qs

CREATED_AT_FORMAT = '%m/%d/%Y %H:%M:%S'

TRANSFORMS = {
    #'created_at'    : lambda s: s.strftime(CREATED_AT_FORMAT),
    'to_user_name'  : lambda s: (s,'')[s == None],
    'source'        : lambda s: HTMLParser().unescape(s),
    'metadata'      : lambda s: urlencode(s),
    }

GEOS = {
    'type'      : None,
    'latitude'  : None,
    'longitude' : None,
    }

REPLACEMENTS = [
    ('\n', ' '),
    ('\t', ' '),
    ('"', "'")
    ]

# csv file globals
DELIM    = ','
QUOTING  = csv.QUOTE_NONNUMERIC
ENCODING = 'utf-8'
ENCODINGS = [
    'utf-8',
    'cp1250'
    ]

# pretty format for print statements
DT_PRETTY = '%m-%d-%Y %H:%M:%S'

# main()
def main(query, configs, params, ignores, output, url,
         transforms=TRANSFORMS, replacements=REPLACEMENTS, is_lower=True):

    print "Starting to get twitters: %s" % ','.join(query)

    # if the is_lower() is True
    qs = query
    if is_lower: queries = [q.lower() for q in qs]

    # to be explicit, these are the tools configs.
    # how many attempts, how many pages, and how long to wait
    # between attempts
    print "Setting up tool configurations"
    max_attempts = int(configs['attempts'])
    max_pages    = int(configs['pages'])
    wait         = int(configs['wait'])
    datetime_format = configs['format']

    # make sure params are set
    params['format'] = 'json'   # until we can parse other formats

    # get existing results
    csv_file = output['name']
    existing_tweets = read_csv(csv_file)

    # get existing IDS, and add to the since_id params
    ids = get_ids(existing_tweets)
    max_ids = get_max_ids(ids)
    query_params = {}
    for q in qs:
        query_params[q] = params
        if max_ids.has_key(q): query_params[q]['since_id'] = max_ids[q]

    # get the tweets and write to the output file
    pages = 0
    count = 0
    print "Starting twitter scrapping now: %s" % datetime.datetime.now().strftime(DT_PRETTY)
    while True:
        # grab tweets from twitter
        tweets = get_all_tweets(queries, query_params, url, wait, max_attempts)

        # clean tweets
        tweets, twitter_data = clean_tweets(tweets, ids, transforms, replacements, datetime_format)

        # write to csv file
        write_csv(csv_file, tweets)
        count += len(tweets)

        # get new params
        query_params = set_new_params(twitter_data, params)

        # continue until we have no params
        if len(query_params) == 0:
            print "No more 'next_page' keys found, no more twitters to scrape"
            break

        # continue until we hit the max pages
        if pages == max_pages-1:
            print "Max pages '%d' reached, stopping." % max_pages
            break

        # make sure we do not duplicate the twitter data
        # get existing results
        existing_tweets = read_csv(csv_file)

        # get existing IDS, and add to the since_id params
        ids = get_ids(existing_tweets)

        pages += 1
        print "Getting results for page: %d" % pages

    print "Wrote '%d' tweets to: %s" % (count, csv_file)
    print "Completed at: %s" % datetime.datetime.now().strftime(DT_PRETTY)

# set up the new params, which we get from the next_page key on the
# left over twitter data
def set_new_params(twitter_data, original_params, key='next_page'):
    params = {}
    for q, keys in twitter_data.items():
        # skip missing queries
        if not twitter_data.has_key(q): continue

        # setup the 'local' params
        p = twitter_data[q]

        # remove all queries that do not have a next page
        if not p.has_key(key): continue

        # the next_page value comes with a '?' as the first character
        np = p[key][1:]

        # convert to dictionary
        np = parse_qs(np)

        # now add to params
        params[q] = np
        for k,v in original_params.items():
            if params[q].has_key(k): continue
            params[q][k]=v
    return params

# get all the twitter data
def get_all_tweets(queries, params, url,wait,  max_attempts):
    """ get all of the twitter data """
    qs = queries
    # create the query strings
    queries = build_queries(queries, params, url)

    # get twitter data
    tweets = get_tweets(queries, max_attempts, wait)
    return tweets

# build the query strings
# return a tuple of the queries and the tweets (yeilded)
def build_queries(query, params, url):
    urls = []
    for q in query:
        if not params.has_key(q): continue
        p = params[q]
        # this lets us know to stop
        if p == None: continue

        # make sure that we are always using the same query
        p['q'] = q
        # make sure params are NOT lists
        for k,v in p.items():
            if not type(v) is list: continue
            p[k] = v[0]
        u = '%s?%s' % (url, urlencode(p))
        u = u.replace('None','')
        print "[%s] query: %s" % (q, u)
        urls.append(u)
    return urls

# get the tweets, unload the jsons
def get_tweets(queries, attemps=10, wait=5):
    """ get the tweets, and unload the json object """
    for q in queries:
        tweet_results = get_tweet(q, attemps, wait)
        json_results = unload_json(tweet_results)
        if json_results == None: continue
        yield json_results

# get_tweets:
#   grab all twitter results (100) from the twitter search
def get_tweet(query, attempts=10, wait=5):
    count = 0
    while True:
        try:
            tweet = urllib2.urlopen(query)
        except urllib2.HTTPError, herr:
            count += 1
            print 'Attempt %d: Access denied, sleeping for %d seconds' % (count, wait),
            sleeper(wait)
            continue
        if count > attempts:
            print 'maximum attempts have been reached (%d) for query: %s' % (attempts, query)
            break
        break
    print "Found tweets for query '%s'" % query
    return tweet

# quick little progress bar for sleeper, it is seems more usefule to have
# something showing that the tool is working instead
def sleeper(wait=5):
    for i in xrange(0,wait):
        sys.stdout.write('.')
        sleep(i)
    print ''

# unload the json object, or raise an exception if it fails
def unload_json(json_obj):
    try:
        results = simplejson.load(json_obj)
    except simplejson.decoder.JSONDecodeError, jerr:
        # just return the empty dict.
        #raise jerr
        results = None
    return results

# read the tweets from the queries, seperate the results from the rest of
# the data and return taht as a seperate dictionary
def clean_tweets(all_tweets, ids, transforms=TRANSFORMS, replacements=REPLACEMENTS, datetime_format=CREATED_AT_FORMAT):
    """ clean tweets, fixing url encodings, decoding correctly, and seperating the results
        from the rest of the twitter data
    """
    results = []
    other_twitter_data = {}
    queries = []
    for tweets in all_tweets:
        d = tweets
        # convert the query value back from the url encoding
        q  = urllib2.unquote(tweets['query'])
        if q in queries: continue
        queries.append(q)

        # get the ids, we want to not duplicate ids based on the same
        # query
        q_ids = []
        if ids.has_key(q): q_ids = ids[q]

        # read the contents
        tweets = clean_query_results(tweets, q, q_ids, transforms, replacements, created_at_format=datetime_format)

        # add to the non-results data
        del d['results']
        other_twitter_data[q] = d

        # add results
        results += tweets

    results = [r for r in results if len(r) != 0]

    # cleaned results
    print "Cleaned '%d' tweets for queries: %s" % (len(results), ','.join(queries))
    return results, other_twitter_data

# clean the results, using the transforms.
def clean_query_results(tweet, query, query_ids, transforms, replacements, key='results', encoding=ENCODING, encodings=ENCODINGS, created_at_format=CREATED_AT_FORMAT):
    results = tweet[key]
    data    = []
    count   = 0
    for l in results:
        l['query'] = query
        q_id = long(l['id'])
        if q_id in query_ids: continue

        # use transforms and transform data
        for k,v in l.items():
            if transforms.has_key(k): v = transforms[k](v)
            if v == None: v = ''
            v = decoder(v, encodings)

        # clean the text (since it is being outputed to a csv)
        l['text'] = remove_unwanted_chars(l['text'], replacements)

        # the 'created_at' needs some special care
        l = convert_created_at(l, created_at_format)

        # fix the geo coordinates
        l = fix_geo(l)

        # add to data
        count += 1
        data.append(l)

    print "[%s]: Adding '%d' new tweets to results" % (query, count)
    return data

# in order to manage the different possible decodings, walk through different
# encodings to figure out which one works. Otherwise jsut do a repr()
def decoder(val, encodings=ENCODINGS):
    for e in encodings:
        try:
            return val.decode(e)
        except:
            # keep iterating til the correct encoding is discovered
            continue
    # otherwise just do a repr()
    val = repr(val)
    return val

# sometimes the tweet text has characters that will interfere with
# the csv output, remove those characters.
def remove_unwanted_chars(s, replacements):
    for r in replacements:
        try:
            s = s.replace(r[0], r[1])
        except:
            # if not string, just skip it (do not try to guess what it is)
            pass
    return s

# convert the create_at field, since it needs some very very
# special care. Let the lambdas convert back to a string (so
# the user can specify their own formats in the yaml)
def convert_created_at(line,created_at_format=CREATED_AT_FORMAT):
    """ fix the created_at time since it is 'RFC 2822' """
    created_at = line['created_at']
    # convert the tuple to a list, so we can pop the tz out of it.
    c = list(parsedate_tz(created_at))
    tz = c.pop(-1)
    dt = datetime.datetime.fromtimestamp(mktime(c))
    line['created_at'] = dt.strftime(created_at_format)
    line['created_at_shift'] = tz
    return line

# geos come with coordinate type, latitude and longitude (when they do
# exist).
def fix_geo(line):
    # setup the geo default coordinates (they are nothing, because the
    # output is a csv file).
    geo = {
        'type'      : '',
        'latitude'  : '',
        'longitude' : '',
        }
    l_geo = line['geo']
    # if geo is NOT none, then break down the geo
    if not l_geo == None:
        geo['type']      = l_geo['type']
        geo['latitude']  = l_geo['coordinates'][0]
        geo['longitude'] = l_geo['coordinates'][1]

    # add the geo coordinates back onto the line
    for k,v in geo.items(): line[k] = v

    # remove the original geo, since it has been broken down
    del line['geo']
    return line

# --- CSV function
# read the existing csv
def read_csv(fn, delim=DELIM, encoding=ENCODING):
    """ read the csv file, if it exists """
    if not os.path.isfile(fn):
        return csv.DictReader('',delimiter=delim)
    # read csv
    try:
        #reader = csv.reader(open(fn, 'rb', ENCODING), delimiter=delim)
        print "Reading file: %s" % fn
        reader = csv.DictReader(open(fn, 'rb', ENCODING), delimiter=delim)
    except Exception, err:
        raise err
    return reader

# write to the csv file
def write_csv(fn, data, quoting=QUOTING, delim=DELIM, encoding=ENCODING, encodings=ENCODINGS):
    # set the file options
    mode = 'wb'
    add_headers = True
    if len(data) == 0:
        print "No data to add to the file: %s" % fn
        return 0
    headers = get_headers(data)
    # check to see if the file exists
    if os.path.isfile(fn):
        mode = 'ab'
        add_headers = False
        # get headers here if the file exists
        headers = get_headers_from_csv(fn, delim=delim)
    csv_writer = csv.writer(open(fn, mode, encoding=encoding), delimiter=delim, quoting=quoting)
    if add_headers: csv_writer.writerow(headers)

    # write to csv file
    ln = len(data)
    count = 0
    for l in data:
        row = []
        for h in headers:
            v = ''
            if l.has_key(h): v = l[h]
            v = decoder(v, encodings)
            row.append(v)
        csv_writer.writerow(row)
        count += 1
    print 'processed %d of %d tweets' % (count, ln)
    return count
#---

# since the csv functions use DictReader and DictWriter
# the header is just the fieldnames
def get_headers_from_csv(fn, delim=DELIM, encoding=ENCODING):
    f = read_csv(fn, encoding=encoding, delim=delim)
    return f.fieldnames

# get the headers for the csv file from the data
def get_headers(data, pk='id'):
    h = []
    for l in data:
        for k in l.keys():
            if k in h: continue
            h.append(k)
    # make sure the headers are unique
    h = list(set(h))

    # sort the results
    h.sort()

    # make sure the first element is the pk
    h.pop(h.index(pk))
    h.insert(0,unicode(pk))
    return h

# get all of the IDs from the exiting results
def get_ids(data):
    results = {}
    for l in data:
        q = l['query']
        i = l['id']
        if not results.has_key(q): results[q] = []
        results[q].append(i)
    # make sure that they are sorted and unique
    for k,v in results.items():
        v = list(set(v))
        # the results are expected to be sorted and unique, but
        # you never really know
        v.sort()
        results[k] = v
    return results

# it would be nice to assume the ids are always
def get_max_ids(ids):
    max_ids = {}
    for k,v in ids.items():
        max_id = 0
        for i in v:
            if long(i) > max_id: max_id = long(i)
        # if max_id is 0, then there were no results
        # do not add to the max_ids
        if max_id == 0: continue
        max_ids[k] = max_id
    return max_ids

# load the file stream and make make sure that the stream
# is OK (really make sure we wrap queries in quotes)
def load_yaml(stream):
    y = _check_stream(stream)
    try:
        y = yaml.load(y)
    except Exception, err:
        raise err
    return y

# in order to deal load a yaml as without breaking on the '#' or '@'
# characters, we need to just fix the string before we pass it off to
# the yaml.
# Since it needs to be opened, we might as well open it here.
def _check_stream(stream):
    s = stream.read()
    chars = ['#', '@']
    re_query = '^\s\-\s|\%s(\w*)'
    for c in chars:
        r = re.compile(re_query % c)
        r = [res for res in r.findall(s) if not len(res) == 0]
        # there is a better way to do this, with regexp, but I failed to figure
        # it out.
        for res in r:
            res = c + res
            # need to make sure that double quotation characters are replaced with singles
            s = s.replace(res, "'%s'" % res).replace("''", "'").replace('""','"')
    return s

# inputs are provided by the yaml, and compared against the defaults
# if they are not declared by the default then they the default value
# is used
def check_defaults(inputs, defaults):
    for parent_key, items in defaults.items():
        # if the the inputs dictionary is missing the section
        # then add it from defaults
        if not inputs.has_key(parent_key):
            inputs[parent_key] = items
            continue
        # the section of keys exists, check that all the keys
        # are set to either default or the inputs version
        for k,v in items.items():
            if inputs[parent_key].has_key(k): continue
            inputs[parent_key][k] = v
    return inputs

# open overwrite
def open(fn, mode, encoding=ENCODING):
    return codecs.open(fn, mode, encoding=encoding)

# usage()
def usage(args=sys.argv[1:]):
    parser = ArgumentParser(description=__doc__)
    parser.add_argument('query_yaml', type=FileType('rt'),
                            metavar='QUERY_YAML',
                            help='Query.yaml file to read the query and settings from')
    optionals = parser.add_argument_group('optional')
    optionals.add_argument('-c', '--config', dest='configs', type=FileType('rt'),
                            default=os.path.join('configs', 'get-twitters.yaml'),
                            metavar='get-twitters.yaml',
                            help='default settings for the tool')
    opts = parser.parse_args(args)
    return opts


# -- start
if __name__ == '__main__':
    opts = usage()
    yaml_opts    = load_yaml(opts.query_yaml)
    configs_opts = load_yaml(opts.configs)

    # add all options, default and set, to the yamls_opts
    yaml_opts = check_defaults(yaml_opts, configs_opts)

    # run get-twitters
    main(**yaml_opts)
