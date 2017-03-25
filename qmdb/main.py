import requests

def imdbid_to_rturl(imdbid):
    imdbid_str = imdbid.zfill(7)
    omdb_url = 'http://www.omdbapi.com/?i=tt' + imdbid_str + '&tomatoes=true'
    r = requests.get(omdb_url)
    print r

imdbid_to_rturl(3315342)
