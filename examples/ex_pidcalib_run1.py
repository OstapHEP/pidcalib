#!/usr/bin/env python
# -*- coding: utf-8 -*-
# =============================================================================
## @file ex_pidcalib_run1.py
#  An example of simple script to run PIDCalib machinery
#  Use '-h' option to know more
#
#  @code
#  ex_pidcalib_run1.py P -s 21 21r1  -p MagUp -c 'P_hasRich==1' 
#  @endocode
#
#  @author Vanya BELYAEV Ivan.Belyaev@itep.ru
#  @date   2014-05-10  
# =============================================================================
""" An example of simple script to run PIDCalib machinery for Run 1

> ex_pidcalib_run1.py P -s 21 21r1  -p MagUp -c 'P_hasRich==1'

Use '-h' option to know more 

"""
# =============================================================================
__version__ = "$Revision$"
__author__  = "Vanya BELYAEV Ivan.Belyaev@itep.ru"
__date__    = "2011-06-07"
__all__     = ()
# =============================================================================
from  pidcalib.pidcalib1 import run_pid_calib as run_pid_calib1
import ROOT
ROOT.PyConfig.IgnoreCommandLineOptions = True
# =============================================================================
from   ostap.logger.logger import getLogger
if '__main__' == __name__ : logger = getLogger ( 'pidcalib_run1' )
else                      : logger = getLogger ( __name__        )
# =============================================================================
## the actual function to fill PIDcalib histograms 
#  - it books two histogram  (3D in this case)
#  - it fill them with 'accepted' and 'rejected' events (3D in this case)
#  - update input historgams
#  @author Vanya BELYAEV Ivan.Belyaev@itep.ru
#  @date   2014-05-10
def  PION ( particle         , 
            dataset          ,
            plots    = None  ,
            verbose  = False ) :
    """
    The actual function to fill PIDCalib histograms
    - it books two histograms (3D in this case)
    - it fill them with 'accepted' and 'rejected' events (3D in this case)
    - update input historgams
    """
    #
    ## we need here ROOT and Ostap machinery!
    #
    from   ostap.core.pyrouts  import hID
    from   ostap.histos.histos import h3_axes  
    import ROOT
    
    # 
    ## the heart of the whole game:   DEFINE PID CUTS! 
    # 
    accepted = 'Pi_V2ProbNNpi>0.1'  ## ACCEPTED sample 
    rejected = 'Pi_V2ProbNNpi<0.1'  ## REJECTED sample 

    if verbose :        
        logger.info ( "ACCEPTED: %s" % accepted ) 
        logger.info ( "REJECTED: %s" % rejected ) 

    #
    ## book histograms:
    # 
    vlst = ROOT.RooArgList ()
    vlst.add ( dataset.Pi_P    ) ## VARIABLE 
    vlst.add ( dataset.Pi_Eta  ) ## VARIABLE 
    vlst.add ( dataset.nTracks ) ## VARIABLE
    
    #
    ## DEFINE PROPER BINNING FOR THE HISTOGRAMS 
    #
    ## binning in P 
    pbins    = [ 3.2  , 6  , 9  ,  15 , 20  , 30  , 40 , 50 , 60 , 80 , 100 , 120 , 150 ]
    pbins    = [ p*1000 for p in pbins ]

    ## binning in ETA 
    hbins    = [ 2.0 , 2.5 , 3.0 , 3.5 , 4.0 , 4.5 , 4.9 ]
    
    ## binning in #tracks 
    tbins    = [0, 150 , 250 , 400 , 1000]

    #
    ## book 3D-histograms
    #
    ha       = h3_axes ( pbins , hbins , tbins , title = 'Accepted(%s)' % accepted ) 
    hr       = h3_axes ( pbins , hbins , tbins , title = 'Rejected(%s)' % rejected )

    #
    ## fill them:
    #
    ha = dataset.fillHistogram ( ha , vlst , accepted )
    hr = dataset.fillHistogram ( hr , vlst , rejected )

    #
    ## prepare the output
    #
    if not plots : ## create output plots
        
        ha.SetName ( ha.GetTitle() )
        hr.SetName ( hr.GetTitle() )        
        plots = [ ha , hr ]
        
    else         : ## update output plots
        
        plots [0] += ha
        plots [1] += hr 
        ha.Delete ()
        hr.Delete ()
        if ha : del ha
        if hr : del hr
        
    return plots   ## return plots 


# =============================================================================
## the actual function to fill PIDcalib histograms 
#  - it books two histogram  (3D in this case)
#  - it fill them with 'accepted' and 'rejected' events (3D in this case)
#  - update input historgams
#  @author Vanya BELYAEV Ivan.Belyaev@itep.ru
#  @date   2014-05-10
def  PROTON ( particle         , 
              dataset          ,
              plots    = None  ,
              verbose  = False ) :
    """
    The actual function to fill PIDCalib histograms
    - it books two histogram  (3D in this case)
    - it fill them with 'accepted' and 'rejected' events (3D in this case)
    - update input historgams
    """
    #
    ## we need here ROOT and Ostap machinery!
    #
    from   ostap.core.pyrouts  import hID
    from   ostap.histos.histos import h3_axes  
    import ROOT
    
    #
    ## the heart of the whole game: 
    # 
    accepted = 'P_V2ProbNNpi>0.1'  ## ACCEPTED sample 
    rejected = 'P_V2ProbNNpi<0.1'  ## REJECTED sample 

    if verbose :        
        logger.info ( "ACCEPTED: %s" % accepted ) 
        logger.info ( "REJECTED: %s" % rejected ) 

    #
    ## book histograms:
    # 
    #
    ## binning
    #
    ## binning in P 
    pbins    = [ 10 ,  15 , 20  , 30  , 40 , 50 , 60 , 80 , 100 , 120 , 150 , 200 ]

    ## binning in ETA 
    hbins    = [ 2.0 , 2.5 , 3.0 , 3.5 , 4.0 , 4.5 , 4.9 ]
    
    ## binning in #tracks 
    tbins    = [0, 150 , 250 , 400 , 1000]

    #
    ## book histograms
    #
    ha = h3_axes ( pbins , hbins , tbins , title = 'Accepted(%s)' % accepted ) 
    hr = h3_axes ( pbins , hbins , tbins , title = 'Rejected(%s)' % rejected )

    hx =  ROOT.TH1D ( hID() , "Momentum"       , 200 , 0   , 200  )
    hy =  ROOT.TH1D ( hID() , "Pseudorapidity" , 40  , 1.5 , 5.5  )
    hz =  ROOT.TH1D ( hID() , "#tracs"         , 240 , 0   , 1200 )
    
    #
    ## fill them:
    #

    ## vlst = ROOT.RooArgList ()
    ## vlst.add ( dataset.P_P     ) ## VARIABLE 
    ## vlst.add ( dataset.P_Eta   ) ## VARIABLE 
    ## vlst.add ( dataset.nTracks ) ## VARIABLE

    vlst = [ dataset.P_P.name + '/1000' ,  dataset.P_Eta.name , dataset.nTracks.name ]

    hx = dataset.project ( hx , vlst[0]  )
    hy = dataset.project ( hy , vlst[1]  )
    hz = dataset.project ( hz , vlst[2]  )

    vlst.reverse()
    
    ha = dataset.project ( ha , vlst    , accepted )
    hr = dataset.project ( hr , vlst    , rejected )

    results = ha , hr , hx , hy , hz 
    #
    ## prepare the output
    #
    if not plots : ## create output plots
        
        ha.SetName ( ha.GetTitle() )
        hr.SetName ( hr.GetTitle() )        
        plots = results 
        
    else         : ## update output plots

        for i, j in zip ( plots , results ) : i += j


    return plots   ## return plots 


# =============================================================================
if '__main__' == __name__ :

    logger.info ( 80*'*'   )
    logger.info ( __doc__  )
    logger.info ( 80*'*'   )
    logger.info ( ' Author  : %s' %         __author__    ) 
    logger.info ( ' Version : %s' %         __version__   ) 
    logger.info ( ' Date    : %s' %         __date__      )
    logger.info ( ' Symbols : %s' %  list ( __all__     ) )
    logger.info ( 80*'*'   )

    #
    ## import function from Ostap
    #

    ## use it!
    run_pid_calib1 ( PROTON , 'PIDCALIB1.db' , args = [] ) 

    logger.info ( 80*'*' )

# =============================================================================
##                                                                      The END 
# =============================================================================
