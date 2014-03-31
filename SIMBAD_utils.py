"""Module implementing methods for SIMBAD.

Methods implemented in this module:
- doPositionQuery()
- doObjectQuery()

For 'doPositionQuery' method:
Given a set of input parameters, the client sends a query
to the specified SIMBAD server. If the query is executed
successfully, the result will return Python list of SIMBAD
internal objects IDs. If the query fails, the error message 
will be captured.

The input parameters are as follows:
A. General parameters (all optional):
- 'URL'         this parameter will change the default server
                    to be queried ('simbad.harvard.edu')
                    This parameter is set during the instantiation
                    of the client object 'Client(URL='...')'
- proxy         this parameter will set a proxy server
- debug             turning debug on will provide more verbose output
                    This parameter is set during the instantiation
                    of the client object 'Client(debug=1)'

B. For coordinate query:
- 'pstring'         right ascension and declination for coordinate
                    query. Coordinates can be written in sexagesimal,
                    with spaces as field separators. A search radius can
                    be specified using a colon, and given either
                    sexigesimally or decimally. Its default value
                    is 2arcmin.
                    Examples:
                      05 23 34.6 -69 45 22:0 6
                      05 23 34.6 -69 45 22:0.166666
- 'frame'           parameter to change the default 'frame' (ICRS).
                    Valid values are: ICRS, FK4, FK5, GAL, SGAL, ECL
- 'equinox'         parameter to change the default 'equinox' (2006.7)
- 'epoch'           paramater to change the default 'epoch' (J2000)

C. For object query:
- 'obect_names'      Object names to get SIMBAD identifier for

Example:

    >>> from SIMBAD_utils import Client as Client
    >>> SimbadClient = Client(URL="http://simbad.u-strasbg.fr",debug=1)
    >>> SimbadClient.pstring = "05 23 34.6 -69 45 22:0.16667"
    >>> SimbadClient.doPositionQuery()

"""
import re
import sys
import time
import requests

class NoQueryElementsError(Exception):
    pass

class IncorrectInputError(Exception):
    pass

class Client:
    # alternative: http://simbad.u-strasbg.fr
    def_baseURL = 'http://simbad.harvard.edu'

    def __init__(self, URL=None, proxy=None, debug=0):

        self.debug   = debug
        self.baseURL = URL or self.def_baseURL
        self.proxees = {}
        if proxy:
            self.proxees['http'] = proxy
        self.elements = []
        self.pstring  = ''
        self.radius   = ''
        self.ra       = ''
        self.dec      = ''
        self.equinox  = ''
        self.epoch    = ''
        self.frame    = ''
        self.frames   = ['ICRS','FK4','FK5','GAL','SGAL','ECL']
        self.error    = ''
#        self.__preamble = 'simbad/sim-script?submit=submit+script&script='
        self.__preamble = 'simbad/sim-script'
        self.object   = ''
        self.result   = ''
        self.script   = ''
        self.qFormats = { 'posquery':'%IDLIST(1)',
                          'objquery':'%IDLIST(1)'}
        self.stime    = time.time()

    def doPositionQuery(self):
        '''
        Query SIMBAD with a position string
        '''
        ppat = re.compile('([0-9\.\ ]+)\s+([\-\+][0-9\.\ ]+)')
        rpat = re.compile('([0-9]+)\s+([0-9]+)\s*([0-9]+)?')
        # This parameter will define what information we will get back
        self.qType = 'posquery'
        # The 'script' variable will hold the script to be sent to SIMBAD
        self.script = ''
        # The list 'elements' will hold the various elements used to generate
        # the script
        self.elements = []
        # Set the script header if we don't have it yet
        if len(self.elements) == 0:
            self.__setscriptheader()
        # By now we at least need to have a script header
        if len(self.elements) == 0:
            raise NoQueryElementsError
        # Parse the position string
        if self.pstring:
            pos = re.sub('[\'\"]','',self.pstring)
            # We are expecting a position string of the form
            #  05 23 34.6 -69 45 22:0 6
            # or
            # 05 23 34.6 -69 45 22:0.166666
            # but if there is not colon, we assume no radius
            # was specified
            try:
                radec,rad = pos.split(':')
            except ValueError:
                rad = ''
                radec = pos
            # Figure out how the radius was specified
            rmat = rpat.search(rad)
            if rmat:
                try:
                    rad = "%sh%sm%ss" % (rmat.group(1),rmat.group(2),int(rmat.group(3)))
                except (IndexError, TypeError):
                    if int(rmat.group(1)) > 0:
                        rad = "%sh%sm" % (rmat.group(1),rmat.group(2))
                    else:
                        rad = "%sm" % rmat.group(2)
            # Determine RA and DEC
            pmat = ppat.search(radec)
            try:
                self.ra = pmat.group(1)
                self.dec= pmat.group(2)
            except:
                raise IncorrectInputError, "coordinate string could not be parsed"
            # IF we have a radius, set the radius
            if rad:
                if re.search('m',rad):
                    self.radius = rad
                else:
                    self.radius = "%sd"%rad
        # With RA and DEC, we can do our query. Yell if the format is wrong!
        if self.ra and self.dec:
            if self.dec[0] not in ['+','-']:
                raise IncorrectInputError, "DEC must start with '+' or '-'!"
            if self.radius:
                if self.radius[-1] not in ['h','m','s','d']:
                    raise IncorrectInputError, "radius is missing qualifier!"
                ra = self.ra
                dec= self.dec
                coo_query = 'query coo %s %s radius=%s'% (ra,dec,self.radius)
            else:
                ra = self.ra
                dec= self.dec
                coo_query = 'query coo %s %s'%(ra,dec)
            # Set non-default values, if so specified
            if self.frame and self.frame in self.frames:
                coo_query += " frame %s" % self.frame
            if self.equinox:
                coo_query += " equi=%s" % self.equinox
            if self.epoch:
                coo_query += "epoch=%s" % self.epoch
            # Add the coordinate query to the elements used to form the script
            self.elements.append(coo_query)
        else:
            self.result = ''
            raise IncorrectInputError
        # Form the script from its constituting elements
        self.script = "\n".join(self.elements)
        # Time to do the actual position query
        self.result = self.__doQuery()
        # Check if we got an error back
        if re.search(':error:',self.result):
            if self.debug:
                sys.stderr.write("Returned result:\n%s\n"%self.result)
            self.error = filter(lambda a: len(a) > 0 and a!='XXX',
                             self.result.split('\n'))
            self.error = " ".join(filter(lambda a: not re.search(':::',a),
                                  self.error))
        # If no error was thrown, we should have results. The object IDs returned will
        # be put in a list
        if not self.error:
            self.result = filter(lambda a: len(a) > 0 and a!='XXX',
                                 self.result.split('\n'))
            self.result = map(lambda c: c[1], filter(lambda b: len(b) == 2, map(lambda a: a.split(),self.result)))
        # How long did this all take?
        self.duration = time.time() - self.stime

    def doObjectQuery(self):
        '''
        Query SIMBAD with a object name(s)
        '''
        objects = []
        # This parameter will define what information we will get back
        self.qType = 'objquery'
        # The 'script' variable will hold the script to be sent to SIMBAD
        self.script = ''
        # The list 'elements' will hold the various elements used to generate
        # the script
        self.elements = []
        # Set the script header if we don't have it yet
        if len(self.elements) == 0:
            self.__setscriptheader()
        # By now we at least need to have a script header
        if len(self.elements) == 0:
            raise NoQueryElementsError
        # Parse the position string
        if self.ostring:
            # We are expecting a comma-separated string of objects
            objects = self.ostring.split(',')
        # With a list of object names, we can do our query. Yell if the format is wrong!
        if len(objects) >0:
            obj_query = '\n'.join(objects)
            # Add the coordinate query to the elements used to form the script
            self.elements.append(obj_query)
        else:
            self.result = ''
            raise IncorrectInputError
        # Form the script from its constituting elements
        self.script = "\n".join(self.elements)
        # Time to do the actual position query
        self.result = self.__doQuery()
        # Check if we got an error back
        if re.search(':error:',self.result):
            if self.debug:
                sys.stderr.write("Returned result:\n%s\n"%self.result)
            self.error = filter(lambda a: len(a) > 0 and a!='XXX',
                             self.result.split('\n'))
            self.error = " ".join(filter(lambda a: not re.search(':::',a),
                                  self.error))
        # If no error was thrown, we should have results. The object IDs returned will
        # be put in a list
        if not self.error:
            self.result = filter(lambda a: len(a) > 0 and a!='XXX',
                                 self.result.split('\n'))
            self.result = map(lambda c: c[1], filter(lambda b: len(b) == 2, map(lambda a: a.split(),self.result)))
        # How long did this all take?
        self.duration = time.time() - self.stime

    def __setscriptheader(self):
        '''
        Set the script header for the SIMBAD query. See
        
          http://simbad.u-strasbg.fr/simbad/sim-fscript
          
        for details.
        '''
        self.elements.append('output console=off error=off script=off')
        format = self.qFormats[self.qType]
        self.elements.append('format obj "%s"'%format)
        self.elements.append('result oid')
        self.elements.append('echodata XXX')

    def __doQuery(self):
        '''
        Do the actual position query
        '''
        params = {}
        params['submit'] = 'submit script'
        params['script'] = self.script
        queryURL = "%s/%s" % (self.baseURL,self.__preamble)
        # Do the query, using a proxy if so specified
        r = requests.get(queryURL, params=params, proxies=self.proxees)
        if r.status_code != requests.codes.ok:
            sys.stderr.write('Error while querying SIMBAD. Aborting...\n')
            return
        # Print the query being sent if debugging is on
        if self.debug:
            sys.stderr.write("Query URL: " + r.url + "\n")

        return r.text

if __name__ == '__main__':

#    SimbadClient = Client(URL='http://simbad.u-strasbg.fr',debug=1)
    SimbadClient = Client()
    SimbadClient.debug = 0

#    SimbadClient.pstring = "05 23 34.6 -69 45 22:0 10"
    SimbadClient.pstring = "05 23 34.6 -69 45 22:0.16667"
    SimbadClient.doPositionQuery()
    if not SimbadClient.error:
        print SimbadClient.result
    else:
        print SimbadClient.error

    print "Duration: %s seconds" % SimbadClient.duration
    
    SimbadClient.ostring = "M31,M101,TW Hydrae"
    SimbadClient.doObjectQuery()
    if not SimbadClient.error:
        print SimbadClient.result
    else:
        print SimbadClient.error

    print "Duration: %s seconds" % SimbadClient.duration
