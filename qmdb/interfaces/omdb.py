import requests
import json


def imdbid_to_rturl(imdbid):
    imdbid_str = str(imdbid).zfill(7)
    omdb_url = 'http://www.omdbapi.com/?i=tt' + imdbid_str + '&tomatoes=true'
    r = requests.get(omdb_url)
    tomato_url = json.loads(r.text)['tomatoURL']
    return tomato_url
