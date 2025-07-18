#!/usr/bin/env python
# -*- coding: utf-8 -*-
# =============================================================================
## @file ex_pidcalib22.py#
#  Example how ot use pidcalib2 with ostap : A tiny wrapper for PIDCalib2 machinery 
#  @see https://pypi.org/project/pidcalib2
# =============================================================================
from   ostap.plotting.canvas import use_canvas
from   ostap.core.core       import hID 
from   pidcalib.pidcalib     import PARTICLE_1D, PARTICLE_2D, PARTICLE_3D
import ostap.io.zipshelve    as     DBASE 
import ROOT 
# =============================================================================
from   ostap.logger.logger import getLogger
logger = getLogger( 'example.pidcalib2' )
# =============================================================================
logger.info ( "Use pidcalib2 machinery with ostap" )
# =============================================================================

# 
## 1D efficiency:
#

xvar    = 'log10(probe_P/1000)'
h1D     = ROOT.TH1D    ( hID() , "Efficincy as fcuntion of %s" % xvar , 50  , 0.2 , 2 )
request = PARTICLE_1D ( 'Pi' , ## partclre type
                        'probe_MC15TuneV1_ProbNNpi>0.5 ' , ## criterion to be tested
                        'Turbo18'                        , ## data sample
                        'up'                             , ## magnet polarity
                        ""                               , ## additional cuts
                        h1D                              , ## 1D template histogram
                        xvar                             ) ## the x-axis variable

## 
e1, a1 , r1 = request.process ( progress  = True ,
                                use_frame = True ,
                                parallel  = True ) 


with DBASE.open ( 'pidcalib.db' ) as db :
    
    tag = '%s/%s/%s' % ( request.particle , request.sample , request.magnet )
    
    ## save results 
    db [ 'results:%s' % tag ] = e1 ,a1 , r1
    
    ## save also the request  (can be reprocesses later, if needed
    db [ 'request:%s' % tag ] = request 
    
    db.ls() 

with use_canvas ( 'Efficiency' , wait = 2 ) :  e1.draw()
    

# ==========================================================================================
##                                                                                   The END
# ==========================================================================================
