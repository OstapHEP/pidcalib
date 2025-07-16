#!/usr/bin/env python
# -*- coding: utf-8 -*-
## @file  pidgen.py
#  A tiny wrapper for Anton Poluektov's pidgen2/pidcorr machinery
#  @see https://pypi.org/project/pidgen2
#  It allows to add the new resampled variables directly into the input TTree/TChain
# ==========================================================
#
#
# *******************************************************************************
# *                                                                             *
# *     ooooooooo.    o8o        .o8    .oooooo.                                *
# *     `888   `Y88.  `"'       "888   d8P'  `Y8b                               *
# *      888   .d88' oooo   .oooo888  888           .ooooo.  oooo d8b oooo d8b  * 
# *      888ooo88P'  `888  d88' `888  888          d88' `88b `888""8P `888""8P  *
# *      888          888  888   888  888          888   888  888      888      *
# *      888          888  888   888  `88b    ooo  888   888  888      888      *
# *     o888o        o888o `Y8bod88P"  `Y8bood8P'  `Y8bod8P' d888b    d888b     *
# *                                                                             *
# *******************************************************************************
#
#  Correct the input simulated PID response `input-var` for track with the given pt/eta/#tracks caracteristic 
#  using the calibration sample `sample` for certain data-taking period `dataset`
#  
#  Usage is fairly transparent:
#
#  @code
#
#  ## input MC data to be corrected 
#  data_2016u = ROOT.TChain ( ... )
#  data_2016u = ROOT.TChain ( ... )
#
#  ## Define the requests as  ( input-data , [ corrections ] ) 
#  requests = [
#                             input-var     corrected-var      sample            dataset         calibration var          pt in MeV       pseudorapidity    #tracks          
#   ( data_2016u , [ Correct ( 'pid_pi1' , 'pid_pi1_corr' , 'pi_Dstar2Dpi' , 'MagUp_2016'    , 'MC15TuneV1_ProbNNpi' , 'pt_pion[0]*1000' , 'eta_pion[0]' , 'nTracks' ) ,
#                    Correct ( 'pid_pi2' , 'pid_pi2_corr' , 'pi_Dstar2Dpi' , 'MagUp_2016'    , 'MC15TuneV1_ProbNNpi' , 'pt_pion[1]*1000' , 'eta_pion[1]' , 'nTracks' ) ] ) ] 
#   ( data_2016d , [ Correct ( 'pid_pi1' , 'pid_pi1_corr' , 'pi_Dstar2Dpi' , 'MagDown_2016'  , 'MC15TuneV1_ProbNNpi' , 'pt_pion[0]*1000' , 'eta_pion[0]' , 'nTracks' ) ,
#                    Correct ( 'pid_pi2' , 'pid_pi2_corr' , 'pi_Dstar2Dpi' , 'MagDown_2016'  , 'MC15TuneV1_ProbNNpi' , 'pt_pion[1]*1000' , 'eta_pion[1]' , 'nTracks' ) ] ) ] 
#  ]
#
#  ##
#  pcorr = PidCorr ( simversion = 'Sim09' , ... )
#  pcorr.process ( requests , report = True , progress = True , parallel = True ) 
#
#  @endcode
#
#    `PidCorr` feeds the `pidgen2.correct` function with following arguments 
#        - `input` 
#        - `sample`
#        - `dataset`
#        - `variable`
#        - `branches`
#        - `output`
#        - `outtree` 
#        - `pidcorr`
#        - `friend` 
#
#  All other arguments can be provided via `PidCorr` constructor
#    
#  Input data-chains can be created manualy (as shown above) or 
#  using `ostap.trees.data.Data` utility 
#
#  IMPORTANT: PidGen&PidCorr  UPDATE  the input data, therefore
#   - One can *not* process the input data via the READ-ONLY protocols! 
#   - CERN'  /eos/  is *NOT* reliable storage for modifications of data!
#     ROOT tends to segfault when ROOT files on /eos/ are modified 
#
#  @see    ostap.trees.data.Data
#  @author Vanya BELYAEV Ivan.Belyaev@cern.ch
#  @date   2025-07-12
# =============================================================================
"""  A tiny wrapper for Anton Poluektov's pidgen2/pidcorr machinery
- see https://pypi.org/project/pidgen2
 - It allows to add the new resampled variables directly into the input TTree/TChain

  *******************************************************************************
  *                                                                             *
  *     ooooooooo.    o8o        .o8    .oooooo.                                *
  *     `888   `Y88.  `"'       "888   d8P'  `Y8b                               *
  *      888   .d88' oooo   .oooo888  888           .ooooo.  oooo d8b oooo d8b  * 
  *      888ooo88P'  `888  d88' `888  888          d88' `88b `888""8P `888""8P  *
  *      888          888  888   888  888          888   888  888      888      *
  *      888          888  888   888  `88b    ooo  888   888  888      888      *
  *     o888o        o888o `Y8bod88P"  `Y8bood8P'  `Y8bod8P' d888b    d888b     *
  *                                                                             *
  *******************************************************************************

  Correct the input simulated PID response `input-var` for track with the given pt/eta/#tracks caracteristic 
  using the calibration sample `sample` for certain data-taking period `dataset`
  The usage is fairly transparent:
    
  ## input MC-data to be corrected 
    
  >>> data_2016u = ROOT.TChain ( ... )
  >>> data_2016u = ROOT.TChain ( ... )
    
  ## Define the requests as  ( input-data , [ corrections ] ) 
  >>>  requests = [
  ##                                   input-var     corrected-var      sample            dataset       calibration va           pt in MeV         pseudorapidity  #trans          
  >>>  ... ( data_2016u , [ Correct ( 'pid_pi1' , 'pid_pi1_corr' , 'pi_Dstar2Dpi' , 'MagUp_2016'   , 'MC15TuneV1_ProbNNpi' , 'pt_pion[0]*1000' , 'eta_pion[0]' , 'nTracks' ) ,
  >>>  ...                  Correct ( 'pid_pi2' , 'pid_pi2_corr' , 'pi_Dstar2Dpi' , 'MagUp_2016'   , 'MC15TuneV1_ProbNNpi' , 'pt_pion[1]*1000' , 'eta_pion[1]' , 'nTracks' ) ] ) ] 
  >>>  ... ( data_2016d , [ Correct ( 'pid_pi1' , 'pid_pi1_corr' , 'pi_Dstar2Dpi' , 'MagDown_2016' , 'MC15TuneV1_ProbNNpi' , 'pt_pion[0]*1000' , 'eta_pion[0]' , 'nTracks' ) ,
  >>>                       Correct ( 'pid_pi2' , 'pid_pi2_corr' , 'pi_Dstar2Dpi' , 'MagDown_2016' , 'MC15TuneV1_ProbNNpi' , 'pt_pion[1]*1000' , 'eta_pion[1]' , 'nTracks' ) ] ) ] 
  >>> 
    
  ##
  >>> pcorr = PidCorr ( simversion = 'Sim09' , ... ) 
  >>> pcorr.process ( requests , report = True , progress = True , parallel = True ) 
    
  `PidCorr` feeds the `pidgen2.correct` function with following arguments 
     - `input` 
     - `sample`
     - `dataset`
     - `variable`
     - `branches`
     - `output`
     - `outtree` 
     - `pidcorr`
     - `friend` 

  All other arguments can be provided via `PidCorr` constructor

  Input data-chains can be created manualy (as shown above) or 
  using `ostap.trees.data.Data` utility 
    
  IMPORTANT: PigGen&PidCorr update the input data, therefore
   - One can *not* process the input dat via the READ-ONLY protocols
   - CERN /eos is not reliable storage for modifications of dataL
     ROOT tends to segfault when data in /eos/ are modified 

"""
# =============================================================================
__version__ = "$Revision$"
__author__  = "Vanya BELYAEV Ivan.Belyaev@cern.ch"
__date__    = "2025-07-12"
__all__     = (
    'Correct'  , ## helper class to describe the elementary request for PidCorr
    'PidCorr'  , ## run pidgen2 machinery
)
# =============================================================================
## Logging 
# =============================================================================
from   ostap.logger.logger import getLogger
logger = getLogger ( 'ostap.tools.pidcorr' )
# =============================================================================
from pidgen import PidCorr, Correct 

# =============================================================================
if '__main__' == __name__ :
    
    from ostap.utils.docme import docme
    docme ( __name__ , logger = logger )

# =============================================================================
##                                                                      The END 
# =============================================================================

