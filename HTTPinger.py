
import urllib2
from urlparse import urlparse


class HTTPinger:
    def getres(self, url, webuser, webpass):
        scheme, domain, path, x1, x2, x3 = urlparse(url)


        finder = HTTPRealmFinder(url)
        realm = finder.get()
        handler = urllib2.HTTPBasicAuthHandler()
        # spoof a web browser
        headers = { 'User-Agent' : 'Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)' }
        handler.add_password(realm, domain, webuser, webpass)

        opener = urllib2.build_opener(handler)
        urllib2.install_opener(opener)

        try:
            req = urllib2.Request(url,None, headers)
            res = urllib2.urlopen(req)
        except urllib2.HTTPError, e:
            if e.code == 401:
                res = 'Error 401 not authorized'
            elif e.code == 404:
                res = 'Error 404 not found'
            elif e.code == 503:
                res = 'Error 503 service unavailable'
            else:
                res = 'unknown error: '
        return res



class HTTPRealmFinderHandler(urllib2.HTTPBasicAuthHandler):
    def http_error_401(self, req, fp, code, msg, headers):
        realm_string = headers['www-authenticate']

        q1 = realm_string.find('"')
        q2 = realm_string.find('"', q1+1)
        realm = realm_string[q1+1:q2]

        self.realm = realm


class HTTPRealmFinder:
    def __init__(self, url):
        self.url = url
        scheme, domain, path, x1, x2, x3 = urlparse(url)

        handler = HTTPRealmFinderHandler()
        handler.add_password(None, domain, 'foo', 'bar')
        self.handler = handler

        opener = urllib2.build_opener(handler)
        urllib2.install_opener(opener)

    def ping(self, url):
        try:
            urllib2.urlopen(url)
        except urllib2.HTTPError, e:
            pass

    def get(self):
        self.ping(self.url)
        try:
            realm = self.handler.realm
        except AttributeError:
            realm = None

        return realm

    def prt(self):
        return self.get()

if __name__ == '__main__':
    pinger = HTTPinger()
    print pinger.ping('http://example.com/nonexistent/path', 'username', 'password')
    finder = HTTPRealmFinder('http://www.marine.csiro.au/~cow074/quota/data/IOTA_monthly.nc.gz')
    finder.prt()
