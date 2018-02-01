try:
    import httplib
except ModuleNotFoundError:
    import http.client as httplib


def no_internet():
    conn = httplib.HTTPConnection("www.google.com", timeout=5)
    try:
        conn.request("HEAD", "/")
        conn.close()
        return False
    except:
        conn.close()
        return True