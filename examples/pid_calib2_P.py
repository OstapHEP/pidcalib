#!/usr/bin/env python
# -*- coding: utf-8 -*-
# =============================================================================
## @file
#  An example of simple script to run LHCb/PIDCalib machinery for Run II samples
#
#  @code
#  pid_calib2.py K -p MagUp -y 2018
#  @endocode
#
#  @author Vanya BELYAEV Ivan.Belyaev@itep.ru
#  @date   2017-05-05
# =============================================================================
""" An example of simple script to run LHCb/PIDCalib machinery for Run-II samples

> pid_calib2.py K -p MagUp -y 2018

"""
# =============================================================================
__version__ = "$Revision$"
__author__  = "Vanya BELYAEV Ivan.Belyaev@itep.ru"
__date__    = "2017-05-05"
__all__     = ()
# =============================================================================
import ostap.core.pyrouts
import ostap.parallel.parallel_project
import ostap.parallel.parallel_statvar
from   pidcalib.pidcalib2               import PARTICLE_3D    as PARTICLE
# =============================================================================
from ostap.logger.logger import getLogger
if '__main__' == __name__: logger = getLogger ( 'pid_calib2' )
else                     : logger = getLogger ( __name__     )
# =============================================================================
import ROOT 
# =============================================================================
ROOT.PyConfig.IgnoreCommandLineOptions = True
# =============================================================================

class PROTON (PARTICLE):
    """The actual function to fill PIDcalib histograms
    - it books two histogram  (3D in this case)
    - it fill them with 'accepted' and 'rejected' events (3D in this case)
    """
    def __init__ ( self , cuts = '' ) :

        PARTICLE.__init__ (
            self     ,
            ## accepted sample
            accepted = 'probe_Brunel_MC15TuneV1_ProbNNp>0.4' , ## ACCEPTED sample
            ## rejected sample
            rejected = 'probe_Brunel_MC15TuneV1_ProbNNp<0.4' , ## REJECTED sample
            ## binning in P
            xbins    = [ 3.2 , 6 , 9 , 12 , 15 , 20 , 25 , 30 , 35 , 40 , 45 , 50 , 60 , 70 , 80 , 90 , 100 , 110 , 120 , 150 ] ,
            ## binning in ETA
            ybins    = [ 2.0 , 2.25 , 2.5 , 2.75 , 3.0 , 3.25, 3.5 , 4.75 , 4.0 , 4.25 , 4.5 , 4.65 , 4.9 ] ,
            ## binning in #tracks
            zbins    = [ 0 , 150 , 250 , 400 , 1000 ] ,
            ## xbins    = [ 0 , 50 , 100 , 150 ] ,
            ## ybins    = [ 0 ,    5 ] ,
            ## zbins    = [ 0 , 1000 ] ,
            ##
            xvar     = 'probe_Brunel_P/1000' ,
            yvar     = 'probe_Brunel_ETA'    ,
            zvar     = 'nTracks'             ,
            ##
            cuts     = 'probe_Brunel_hasRich' & ROOT.TCut ( 'probe_Brunel_PT>180' ) & ROOT.TCut ( cuts ) ,
            ##
            weight   = 'probe_Brunel_sWeight' )

class KAON (PARTICLE):
    """The actual function to fill PIDcalib histograms
    - it books two histogram  (3D in this case)
    - it fill them with 'accepted' and 'rejected' events (3D in this case)
    """
    def __init__ ( self , cuts = '' ) :

        PARTICLE.__init__ (
            self     ,
            ## accepted sample
            accepted = 'probe_Brunel_MC15TuneV1_ProbNNk>0.2' , ## ACCEPTED sample
            ## rejected sample
            rejected = 'probe_Brunel_MC15TuneV1_ProbNNk<0.2' , ## REJECTED sample
            ## binning in P
            xbins    = [ 3.2 , 6 , 9 , 12 , 15 , 20 , 25 , 30 , 35 , 40 , 45 , 50 , 60 , 70 , 80 , 90 , 100 , 110 , 120 , 150 ] ,
            ## binning in ETA
            ybins    = [ 2.0 , 2.25 , 2.5 , 2.75 , 3.0 , 3.25, 3.5 , 4.75 , 4.0 , 4.25 , 4.5 , 4.65 , 4.9 ] ,
            ## binning in #tracks
            zbins    = [ 0 , 150 , 250 , 400 , 1000 ] ,
            ## xbins    = [ 0 , 50 , 100 , 150 ] ,
            ## ybins    = [ 0 ,    5 ] ,
            ## zbins    = [ 0 , 1000 ] ,
            ##
            xvar     = 'probe_Brunel_P/1000' ,
            yvar     = 'probe_Brunel_ETA'    ,
            zvar     = 'nTracks'             ,
            ##
            cuts     = 'probe_Brunel_hasRich' & ROOT.TCut ( 'probe_Brunel_PT>200' ) & ROOT.TCut ( cuts ) ,
            ##
            weight   = 'probe_Brunel_sWeight' )

        
# =============================================================================
if '__main__' == __name__:

    #
    ## import function from Ostap
    #
    from   pidcalib.pidcalib2               import run_pid_calib
    
    ## use it!
    ## run_pid_calib ( KAON, args = [ '-y', '2018', 'K', '-f', '2', '-v', 'v9r2', '-q' ] )


    ## run_pid_calib ( KAON, args = [ '-y', '2018' , 'K', '-f', '100', '-v', 'v9r2', '-q' , '-e' , '-g'] )
    run_pid_calib ( PROTON, args = [ 'P'  ,
                                     '-c' , '9<=probe_Brunel_P/1000 && probe_Brunel_P/1000<=180' , 
                                     '-y' , '2017' , '2018' , 
                                     ## '-y', '2018' , 
                                     '-v', 'v9r1' , 'v9r2' ,
                                     ## '-d' , 
                                     ## '-f' , '50' ,
                                     '-e' , 
                                     '-g' ,
                                     ## '-z'
                                     ] ) 
    
    logger.info ( 80 * '*')

# =============================================================================
##                                                                      The END
# =============================================================================
