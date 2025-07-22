#!/usr/bin/env python
# -*- coding: utf-8 -*-
# =============================================================================
## @file ex_pidgen2.py
#  Exampel hot ot use PidGen: A tiny wrapper for Anton Poluektov's pidgen2 machinery
#  @see https://pypi.org/project/pidgen2
#  - It allows to add the new corrected variables directly into the input TTree/TChain
# =============================================================================
from   ostap.trees.data      import Data
from   ostap.plotting.canvas import use_canvas
import ostap.io.root_file 
import ROOT, random 
# ==========================================================================================
from   ostap.logger.logger import getLogger
# ==========================================================================================
logger = getLogger( 'example.pidgen2' )
# ==========================================================================================
logger.info ( "Example for pidgen2 machinery with ostap" )
# ==========================================================================================
## get input simulated samples (&copy data into temporary location)
# ==========================================================================================
chain_name = 'X2zz/C2'
mc16d = Data ( '/eos/lhcb/wg/BandQ/X2DDstar/DATA/GANGA2/workspace/ibelyaev/LocalXML/294/*/output/MCCharmX.root' , chain_name ).copy_files ( parallel = True ) 
mc16u = Data ( '/eos/lhcb/wg/BandQ/X2DDstar/DATA/GANGA2/workspace/ibelyaev/LocalXML/295/*/output/MCCharmX.root' , chain_name ).copy_files ( parallel = True )
mc18d = Data ( '/eos/lhcb/wg/BandQ/X2DDstar/DATA/GANGA2/workspace/ibelyaev/LocalXML/298/*/output/MCCharmX.root' , chain_name ).copy_files ( parallel = True )
mc18u = Data ( '/eos/lhcb/wg/BandQ/X2DDstar/DATA/GANGA2/workspace/ibelyaev/LocalXML/299/*/output/MCCharmX.root' , chain_name ).copy_files ( parallel = True )

# ===========================================================================================
## Use PidGen to correct variables 
# ===========================================================================================
from pidcalib.pidgen import Correct, PidCorr 


## (1)Sample #ntrack from histogram
hTracks = ROOT.TH1D ( 'h1' , 'Mimic #tracks distribution in DATA', 200 , 0 , 600 )
for i in range ( 50000 ) : hTracks.Fill ( random.gammavariate ( 3.3 , 55 ) )

## (2)Sample @ntracks from aform data sequence
dTracks = tuple ( random.gammavariate ( 3.3 , 55 ) for i in range ( 10000 ) ) 

## (3) real #ntracks distribtion from B+ =>  (psi' -> J/psi pi+ pi-) K+ decays
with ROOT.TFile( '../data/nTracks.root' , 'r' ) as rf :
    hRun2 = rf['nTracks_Run2'].clone() 

requests = [
    ## use expression to specify #ntarcks
    ( mc16d.chain , ( Correct( 'ann_pion[0]' , 'PID_pi1' , 'pi_Dstar2Dpi' , 'MagDown_2016' , 'MC15TuneV1_ProbNNpi' , 'pt_pion[0]*1000' , 'eta_pion[0]' , 'nTracks*1.15' ) , 
                      Correct( 'ann_pion[1]' , 'PID_pi2' , 'pi_Dstar2Dpi' , 'MagDown_2016' , 'MC15TuneV1_ProbNNpi' , 'pt_pion[1]*1000' , 'eta_pion[1]' , 'nTracks*1.15' ) ) ) ,
    ## sample #ntracks from the histogram
    ( mc16u.chain , ( Correct( 'ann_pion[0]' , 'PID_pi1' , 'pi_Dstar2Dpi' , 'MagUp_2016'   , 'MC15TuneV1_ProbNNpi' , 'pt_pion[0]*1000' , 'eta_pion[0]' , hTracks   ) , 
                      Correct( 'ann_pion[1]' , 'PID_pi2' , 'pi_Dstar2Dpi' , 'MagUp_2016'   , 'MC15TuneV1_ProbNNpi' , 'pt_pion[1]*1000' , 'eta_pion[1]' , hTracks   ) ) ) ,
    ## sample #ntracks from some data sequence 
    ( mc18d.chain , ( Correct( 'ann_pion[0]' , 'PID_pi1' , 'pi_Dstar2Dpi' , 'MagDown_2018' , 'MC15TuneV1_ProbNNpi' , 'pt_pion[0]*1000' , 'eta_pion[0]' , dTracks   ) , 
                      Correct( 'ann_pion[1]' , 'PID_pi2' , 'pi_Dstar2Dpi' , 'MagDown_2018' , 'MC15TuneV1_ProbNNpi' , 'pt_pion[1]*1000' , 'eta_pion[1]' , dTracks   ) ) ) ,
    ## real historgam
    ( mc18u.chain , ( Correct( 'ann_pion[0]' , 'PID_pi1' , 'pi_Dstar2Dpi' , 'MagUp_2018'   , 'MC15TuneV1_ProbNNpi' , 'pt_pion[0]*1000' , 'eta_pion[0]' , hRun2     ) , 
                      Correct( 'ann_pion[1]' , 'PID_pi2' , 'pi_Dstar2Dpi' , 'MagUp_2018'   , 'MC15TuneV1_ProbNNpi' , 'pt_pion[1]*1000' , 'eta_pion[1]' , hRun2     ) ) ) ,
]

pcorr = PidCorr ( 'Sim09' ,
                  nTrk_name        = 'nTrk'            , 
                  local_storage    = "./templates/"    ,  # Directory for local template storage, used when the template is not available in 
                  local_mc_storage = "./mc_templates/" ,  # Directory for local template storage, used when the template is not available in
                 ) 
pcorr.process   ( requests , progress = True  , report = True , parallel  = True ) 


## 
with use_canvas ( 'original and corrected/1' , wait = 2 ) : 
    ## draw original/"OLD" pion PID 
    mc16d.chain.draw ( 'ann_pion[0]'      , color = 4 )
    ## draw "CORRECTED" pion PID 
    mc16d.chain.draw ( 'PID_pi1'  , opts = 'same' , color = 2 )

## 
with use_canvas ( 'original vs corrected/2' , wait = 2 ) : 
    ## draw original/"OLD" pion PID 
    mc16d.chain.draw ( 'ann_pion[0] : PID_pi1' , opts = 'box' , color = 4 )

with use_canvas ( 'original vs corrected/3' , wait = 2 ) : 
    ## draw original/"OLD" pion PID 
    mc16d.chain.draw ( 'log10(1-PID_pi1) : log10(1-ann_pion[0])' ,
                       '0<ann_pion[0] && ann_pion[0]<1 && 0<PID_pi1 && PID_pi1<1 ' , opts = 'box' , color = 8 )


# ==========================================================================================
##                                                                                   The END
# ==========================================================================================
