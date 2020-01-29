#!/usr/bin/env python
# -*- coding: utf-8 -*-
# =============================================================================
import os, collections
# =============================================================================
from  ostap.logger.logger import getLogger, setLogging
if '__main__' == __name__: logger = getLogger( 'ostap.Grid')
else                     : logger = getLogger( __name__    )
# =============================================================================

# =============================================================================
## check the validity of GRID proxy
#  @author Vanya Belyaev  Ivan.Belyaev@itep.ru
#  @date 2012-10-10
def hasGridProxy():
    """Check GRID proxy
    >>> hasGridProxy ()
    """
    import os
    from subprocess import Popen, PIPE
    arguments =   'dirac-proxy-info --checkvalid'
    arguments = [ 'dirac-command' ] + arguments.split()
    ## arguments = arguments.split()
    logger.verbose('hasGridProxy:use Popen(%s)' % arguments )
    try : 
        p = Popen ( arguments , stdout = PIPE, stderr = PIPE )
        (cout, cerr) = p.communicate()
    except :
        logger.warning ( "Cannot check Grid proxy" ) 
        return False 
    #
    if 0 != p.returncode: return False
    #
    if 'expired' in cout: return False
    if 'Insane'  in cout: return False
    if 'Error'   in cout: return False
    #
    return 0 == p.returncode and cout and not cerr

# =============================================================================
## @class BKRequest
#  Form a  query for  BK request
#  @param   path      the path in Bookkeeping DB
#  @param   nmax      maximal number of files to pickup
#  @param   first     the index for the first file to pickup
#  @param   last      the index for the last file to pickup
#  @param   grid      list of grid sites to consider
#  @param   accessURL return files as acees URLs ?
#  @param   SEs       list of Storage elements to consider
class BKRequest ( collections.namedtuple('BKRequest', 'path  nmax first last grid accessURL SEs' ) ):
    """Form a  query for  BK request
    - path      : the path in Bookkeeping DB
    - nmax      : maximal number of files to pickup
    - first     : the index for the first file to pickup
    - last      : the index for the last file to pickup
    - grid      : list of grid sites to consider
    - accessURL : return files as acees URLs ?
    - SEs       : list of Storage Elements to consider
    """

    ## construct a request
    def __new__(cls,
                path,
                nmax=-1,
                first=0,
                last=-1,
                grid=[],
                accessURL=False,
                SEs=[]):
        """Form a query for  BK request
        - path      : the path in Bookkeeping DB
        - nmax      : maximal number of files to pickup
        - first     : the index for the first file to pickup
        - last      : the index for the last file to pickup
        - grid      : list of grid sites to consider
        - accessURL : return files as acees URLs ?
        """
        assert path and isinstance  ( path , str ) , \
               "BKRequest: invalid ``path'' %s/%s "  % ( path , type(path) )
        if isinstance(grid, str): grid = [grid]

        path = str(path)
        if isinstance(grid, str): grid = [grid]
        grid = [i for i in grid]
        accessURL = True if accessURL else False
        if isinstance(SEs, str): SEs = [SEs]
        SEs = [i for i in SEs]

        return super(BKRequest, cls).__new__(cls, path, nmax, first, last,
                                             grid, accessURL, SEs)


# =============================================================================
## Get the files from Bookkeeping DB
#  @valid GRID proxy is needed!
#  @code
#  >>> request   = ...
#  >>> files     = filesFromBK ( request )
#  @endcode
def filesFromBK(request):
    """Get the files from Bookkeeping DB
    - valid GRID proxy is needed!

    >>> request   = ...
    >>> files     = filesFromBK ( request )
    """
    if not hasGridProxy():
        logger.error('filesFromBK: No Grig proxy!')
        return []

    if   isinstance ( request , tuple ) : request = BKRequest (  *request )
    elif isinstance ( request , dict  ) : request = BKRequest ( **request )

    path      = request.path
    nmax      = request.nmax
    first     = request.first
    last      = request.last
    grid      = request.grid
    accessURL = request.accessURL
    SEs       = request.SEs

    arguments = 'dirac-command get_files_from_BK'

    arguments += " %s " % path

    if nmax < 0: nmax = 1000000
    if last < 0: last = 1000000
    if nmax < 1000000 : arguments += ' --Max %d' % nmax
    if 0    < first   : arguments += ' --First %d' % first
    if last < 1000000 : arguments += ' --Last  %d' % last
    if accessURL      : arguments += ' -a True '
    #
    if grid and isinstance ( grid , str ) :
        arguments += ' --Sites %s ' % grid
    elif grid and 1 == len(grid):
        arguments += ' --Sites %s ' % grid[0]
    elif grid:
        sg = ','.join(grid)
        arguments += ' --Sites %s ' % sg

    if SEs and isinstance ( SEs , str):
        arguments += ' --SEs %s ' % grid
    elif SEs and 1 == len ( SEs ) :
        arguments += ' --SEs %s ' % SEs [ 0 ]
    elif SEs:
        sg = ','.join(SEs)
        arguments += ' --SEs %s ' % sg

    ## arguments += ' "%s" ' % path
    ## convert to DIRAC

    import os
    from subprocess import Popen, PIPE
    arguments = arguments.split()

    logger.verbose('filesFromBK:use Popen(%s)' % arguments)
    p = Popen(arguments, stdout=PIPE, stderr=PIPE)
    (cout, cerr) = p.communicate()

    if 0 != p.returncode or cerr:
        logger.debug(
            'filesFromBK: error from Popen: %d/%s' % (p.returncode, cerr))
        return []
    
    cout = cout.split('\n')[1:] 
    cout = '\n'.join ( cout )
    
    try:
        
        ## print 'here!', cout
        lst = eval ( cout )
        if not isinstance(lst, list):
            raise TypeError("Invalid list type")
        ##
        
        logger.debug( 'filesFromBK: %s ' % lst  )
        return lst

    except:
        logger.debug("filesFromBK: can't interpret: %s" % cout)

    return []


# =============================================================================
if __name__ == '__main__':

    logger.info(80 * '*')
    logger.info(__doc__)
    logger.info(' Author  : %s ' % __author__)
    logger.info(' Version : %s ' % __version__)
    logger.info(' Date    : %s ' % __date__)
    logger.info(' Symbols : %s ' % list(__all__))
    logger.info(80 * '*')

# =============================================================================
# The END
# =============================================================================
