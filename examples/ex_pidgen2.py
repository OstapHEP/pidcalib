#!/usr/bin/env python
# -*- coding: utf-8 -*-
# =============================================================================
## @file ex_pidgen2.py
#  Example how ot use PidGen: A tiny wrapper for Anton Poluektov's pidgen2 machinery
#  @see https://pypi.org/project/pidgen2
#  - It allows to add the new corrected variables directly into the input TTree/TChain
# =============================================================================
from   ostap.trees.data      import Data
from   ostap.plotting.canvas import use_canvas 
# ==========================================================================================
from   ostap.logger.logger import getLogger
# ==========================================================================================
logger = getLogger( 'example.pidgen2' )
# ==========================================================================================
logger.info ( "Example for pidgen2 machinery with ostap" )
# ==========================================================================================
## colelct some input data files

input_dir  = '/eos/lhcb/wg/BandQ/X2DDstar/DATA/GANGA2/workspace/ibelyaev/LocalXML/'

# ==========================================================================================
## get input simulated samples (&copy data to temporary location)
# ==========================================================================================
chain_name = 'X2zz/C2'
mc16d = Data ( '/eos/lhcb/wg/BandQ/X2DDstar/DATA/GANGA2/workspace/ibelyaev/LocalXML/294/*/output/MCCharmX.root' , chain_name ).copy_files ( parallel = True ) 
mc16u = Data ( '/eos/lhcb/wg/BandQ/X2DDstar/DATA/GANGA2/workspace/ibelyaev/LocalXML/295/*/output/MCCharmX.root' , chain_name ).copy_files ( parallel = True )
mc18d = Data ( '/eos/lhcb/wg/BandQ/X2DDstar/DATA/GANGA2/workspace/ibelyaev/LocalXML/298/*/output/MCCharmX.root' , chain_name ).copy_files ( parallel = True ) 
mc18u = Data ( '/eos/lhcb/wg/BandQ/X2DDstar/DATA/GANGA2/workspace/ibelyaev/LocalXML/299/*/output/MCCharmX.root' , chain_name ).copy_files ( parallel = True )

# ===========================================================================================
## Use PidGen to resample variables 
# ===========================================================================================
from pidcalib.pidgen import ReSample, PidGen

requests = [
    ( mc16d.chain , ( ReSample ( 'pid_pi1' , 'pi_Dstar2Dpi' , 'MagDown_2016' , 'MC15TuneV1_ProbNNpi' , 'pt_pion[0]*1000' , 'eta_pion[0]' , 'nTracks*1.15' ) , 
                      ReSample ( 'pid_pi2' , 'pi_Dstar2Dpi' , 'MagDown_2016' , 'MC15TuneV1_ProbNNpi' , 'pt_pion[1]*1000' , 'eta_pion[1]' , 'nTracks*1.15' ) ) ) , 
    ( mc16u.chain , ( ReSample ( 'pid_pi1' , 'pi_Dstar2Dpi' , 'MagUp_2016'   , 'MC15TuneV1_ProbNNpi' , 'pt_pion[0]*1000' , 'eta_pion[0]' , 'nTracks*1.15' ) , 
                      ReSample ( 'pid_pi2' , 'pi_Dstar2Dpi' , 'MagUp_2016'   , 'MC15TuneV1_ProbNNpi' , 'pt_pion[1]*1000' , 'eta_pion[1]' , 'nTracks*1.15' ) ) ) , 
    ( mc18d.chain , ( ReSample ( 'pid_pi1' , 'pi_Dstar2Dpi' , 'MagDown_2018' , 'MC15TuneV1_ProbNNpi' , 'pt_pion[0]*1000' , 'eta_pion[0]' , 'nTracks*1.15' ) , 
                      ReSample ( 'pid_pi2' , 'pi_Dstar2Dpi' , 'MagDown_2018' , 'MC15TuneV1_ProbNNpi' , 'pt_pion[1]*1000' , 'eta_pion[1]' , 'nTracks*1.15' ) ) ) , 
    ( mc18u.chain , ( ReSample ( 'pid_pi1' , 'pi_Dstar2Dpi' , 'MagUp_2018'   , 'MC15TuneV1_ProbNNpi' , 'pt_pion[0]*1000' , 'eta_pion[0]' , 'nTracks*1.15' ) ,
                      ReSample ( 'pid_pi2' , 'pi_Dstar2Dpi' , 'MagUp_2018'   , 'MC15TuneV1_ProbNNpi' , 'pt_pion[1]*1000' , 'eta_pion[1]' , 'nTracks*1.15' ) ) ) , 
]

pgen = PidGen (
    local_storage    = "./templates/"    ,  # Directory for local template storage, used when the template is not available in 
)
pgen.process ( requests , progress = True  , report = True , parallel  = True ) 


with use_canvas ( 'Sampled PID' , wait = 2 ) : 
    ## draw original/"OLD" pion PID 
    mc16d.chain.draw ( 'pid_pi1'  , color = 4 )

# ==========================================================================================
##                                                                                   The END
# ==========================================================================================
