#!/usr/bin/env python
# -*- coding: utf-8 -*-
# =============================================================================
## @file
#  An example of simple script to run LHCb/PIDCalib machinery for Run II samples
#
#  @code
#  pid_calib2.py Pi -p MagUp -y 2015
#  @endocode
#
#  @author Vanya BELYAEV Ivan.Belyaev@itep.ru
#  @date   2017-05-05
# =============================================================================
""" An example of simple script to run LHCb/PIDCalib machinery for Run-II samples

> pid_calib2.py Pi -p MagUp -y 2015

"""
# =============================================================================
__version__ = "$Revision$"
__author__  = "Vanya BELYAEV Ivan.Belyaev@itep.ru"
__date__    = "2017-05-05"
__all__     = ()
# =============================================================================
import ROOT, cppyy
ROOT.PyConfig.IgnoreCommandLineOptions = True
# =============================================================================
from ostap.logger.logger import getLogger
if '__main__' == __name__: logger = getLogger('pid_calib2')
else                     : logger = getLogger(__name__)
# =============================================================================
import ROOT
import ostap.core.pyrouts
import ostap.parallel.parallel_project 
from   PidCalib2 import PARTICLE_3D    as PARTICLE
# =============================================================================
## the actual function to fill PIDcalib histograms
#  - it books two histogram  (3D in this case)
#  - it fill them with 'accepted' and 'rejected' events (3D in this case)
#  @author Vanya BELYAEV Ivan.Belyaev@itep.ru
#  @date   2014-05-10
class KAON (PARTICLE):
    """The actual function to fill PIDcalib histograms
    - it books two histogram  (3D in this case)
    - it fill them with 'accepted' and 'rejected' events (3D in this case)
    """
    def __init__ ( self, cuts= '' ) :

        PARTICLE.__init__ (
            self     ,
            ## accepted sample
            accepted = '(probe_Brunel_hasRich)&&(probe_Brunel_MC15TuneV1_ProbNNk>0.4)' , ## ACCEPTED sample
            ## rejected sample
            rejected = '(probe_Brunel_hasRich)&&(probe_Brunel_MC15TuneV1_ProbNNk<0.4)' , ## REJECTED sample
            ## binning in P
            xbins    = [ 3.2 , 6 , 9 , 12 , 15 , 20 , 25 , 30 , 35 , 40 , 45 , 50 , 60 , 70 , 80 , 90 , 100 , 110 , 120 , 150 ] ,
            ## binning in ETA
            ybins    = [ 2.0 , 2.25 , 2.5 , 2.75 , 3.0 , 3.25, 3.5 , 4.75 , 4.0 , 4.25 , 4.5 , 4.65 , 4.9 ] ,
            ## binning in #tracks
            zbins    = [ 0 , 150 , 250 , 400 , 1000 ] ,
            ## additional cuts (if any)
            cuts     = 'probe_Brunel_hasRich' & ROOT.TCut ( cuts ) ,
            ## "Accept"-function                              what  to project/draw                                 cuts&weight
            acc_fun  = lambda s,data : data.project ( s.ha , 'nTracks : probe_Brunel_ETA : probe_Brunel_P/1000 ', '(%s)*probe_sWeight' % s.accepted ) ,
            ## "Reject"-function                              what  to project/draw                                 cuts&weight
            rej_fun  = lambda s,data : data.project ( s.hr , 'nTracks : probe_Brunel_ETA : probe_Brunel_P/1000 ', '(%s)*probe_sWeight' % s.rejected ) )

        
# =============================================================================
if '__main__' == __name__:

    #
    ## import function from Ostap
    #
    from   pidcalib.pidcalib2               import run_pid_calib
    
    ## use it!
    run_pid_calib ( KAON, args= [ '-y', '2018', 'K', '-f', '30', '-v', 'v8r0', '-q' ] )
    
    logger.info ( 80 * '*')

# =============================================================================
# The END
# =============================================================================
