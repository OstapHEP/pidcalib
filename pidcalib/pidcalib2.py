#!/usr/bin/env python
# -*- coding: utf-8 -*-
# =============================================================================
## @file  pidcalib2.py
#  Module to run LHCb/PIDCalib machinery with ostap for Run II data
#  @see https://twiki.cern.ch/twiki/bin/view/LHCbPhysics/ChargedPID
#
#  To use it, one needs to define processor
#
#  - If GRID proxy is available (and cvmfs for implicit LHCbDdirac)
#    the input data are taken from GRID,
#    otherwise a direct access to '/eos' is used.
#
# =============================================================================
"""Run LHCb/PIDCalib machinery for RUN-II data
- see https://twiki.cern.ch/twiki/bin/view/LHCbPhysics/ChargedPID

To process calibration samples  one needs to create
 - PROCESSOR : a simple python class that defines the action
               to build  1D,2D or 3D effciciency tables(histograms

Other roptions can be specified via command line options,
use '-h' to get them

see example in the file examples/pid_calib2.py

results are stored in the output database (Defualt is `PIDCALIB2.db`

to access the results use

>>> import ostap.io.zipshelve as DBASE
>>> db = DBASE.open ( 'PIDCALIB2.db' , 'r' )
>>> db.ls() 

for each calibration sample  KEY 

>>> acc, rej = db [ KEY ]  ## ``Accepted'' and ``Rejected'' histograms 
>>> eff      = db [ KEY + ':efficiency']  ## efficiency histogram

``Accepted'' and ``Rejected'' histograms are very useful for combination
                              of several samples
``Efficiency'' histogram is obtained as  eff = 1 / ( 1 + rej / acc )

Other entries in database contains useful information for bookkeeping and debugging
"""
# =============================================================================
__version__ = "$Revision$"
__author__  = "Vanya BELYAEV Ivan.Belyaev@itep.ru"
__date__    = "2011-06-07"
__all__ = (
    'run_pid_calib' ,  ## run pid-calib machinery
    'PARTICLE'      ,  ## the abstract class for the  processing object  
    'PARTICLE_1D'   ,  ## helper object to produce 1D efficiency histogram
    'PARTICLE_2D'   ,  ## helper object to produce 2D efficiency histogram
    'PARTICLE_3D'   ,  ## helper object to produce 3D efficiency histogram
)
# =============================================================================
import ostap.core.pyrouts
import ostap.trees.trees
import ostap.trees.cuts 
import ostap.io.root_file 
import ostap.parallel.parallel_project 
from   ostap.parallel.parallel_statvar import pStatVar 
from   ostap.utils.progress_bar        import progress_bar
from   ostap.utils.timing              import timing 
from   ostap.utils.utils               import chunked 
from   ostap.plotting.canvas           import use_canvas
from   ostap.plotting.style            import useStyle
from   ostap.utils.utils               import wait
from   ostap.core.meta_info            import root_info, ostap_info
import ostap.io.zipshelve              as     DBASE
import ROOT, sys, os, abc, pprint, random  
# =============================================================================
from  ostap.logger.logger import getLogger, setLogging
if '__main__' == __name__: logger = getLogger ( 'ostap.pidcalib2' )
else                     : logger = getLogger ( __name__          )
# =============================================================================
assert (1,6,2) <= ostap_info , 'Ostap verion >= 1.6.3 is required!'
# =============================================================================
ROOT.PyConfig.IgnoreCommandLineOptions = True


# =============================================================================
# PIDCALIB data samples (LHCb Bookeeping DB-paths)
# =============================================================================
bookkeeping_paths = {
    #
    ## pp
    # 
    ## 2015, v4r1 
    'pp/2015/v4r1/MagUp'   : '/LHCb/Collision15/Beam6500GeV-VeloClosed-MagUp/Real Data/Reco15a/Turbo02/PIDCalibTuples4r1/PIDMerge01/95100000/PIDCALIB.ROOT'   , 
    'pp/2015/v4r1/MagDown' : '/LHCb/Collision15/Beam6500GeV-VeloClosed-MagDown/Real Data/Reco15a/Turbo02/PIDCalibTuples4r1/PIDMerge01/95100000/PIDCALIB.ROOT' ,
    ## 2015, v5r1 
    'pp/2015/v5r1/MagUp'   : '/LHCb/Collision15/Beam6500GeV-VeloClosed-MagUp/Real Data/Reco15a/Turbo02/PIDCalibTuples5r1/PIDMerge05/95100000/PIDCALIB.ROOT'   , 
    'pp/2015/v5r1/MagDown' : '/LHCb/Collision15/Beam6500GeV-VeloClosed-MagDown/Real Data/Reco15a/Turbo02/PIDCalibTuples5r1/PIDMerge05/95100000/PIDCALIB.ROOT' ,
    ## 2016, v4r1 
    'pp/2016/v4r1/MagUp'   : '/LHCb/Collision16/Beam6500GeV-VeloClosed-MagUp/Real Data/Reco16/Turbo02a/PIDCalibTuples4r1/PIDMerge01/95100000/PIDCALIB.ROOT'   , 
    'pp/2016/v4r1/MagDown' : '/LHCb/Collision16/Beam6500GeV-VeloClosed-MagDown/Real Data/Reco16/Turbo02a/PIDCalibTuples4r1/PIDMerge01/95100000/PIDCALIB.ROOT' ,
    ## 2016, v5r1 
    'pp/2016/v5r1/MagUp'   : '/LHCb/Collision16/Beam6500GeV-VeloClosed-MagUp/Real Data/Reco16/Turbo02a/PIDCalibTuples5r1/PIDMerge05/95100000/PIDCALIB.ROOT'   , 
    'pp/2016/v5r1/MagDown' : '/LHCb/Collision16/Beam6500GeV-VeloClosed-MagDown/Real Data/Reco16/Turbo02a/PIDCalibTuples5r1/PIDMerge05/95100000/PIDCALIB.ROOT' ,
    ## 2016, v9r3 
    'pp/2016/v9r3/MagUp'   : '/LHCb/Collision16/Beam6500GeV-VeloClosed-MagUp/Real Data/Reco16/Turbo02a/PIDCalibTuples9r3/PIDMerge05/95100000/PIDCALIB.ROOT'   , 
    'pp/2016/v9r3/MagDown' : '/LHCb/Collision16/Beam6500GeV-VeloClosed-MagDown/Real Data/Reco16/Turbo02a/PIDCalibTuples9r3/PIDMerge05/95100000/PIDCALIB.ROOT' ,
    ## 2017, v8r1 
    'pp/2017/v8r1/MagUp'   : '/LHCb/Collision17/Beam6500GeV-VeloClosed-MagUp/Real Data/Reco17/Turbo04/PIDCalibTuples8r1/PIDMerge05/95100000/PIDCALIB.ROOT' ,
    'pp/2017/v8r1/MagDown' : '/LHCb/Collision17/Beam6500GeV-VeloClosed-MagDown/Real Data/Reco17/Turbo04/PIDCalibTuples8r1/PIDMerge05/95100000/PIDCALIB.ROOT' ,
    ## 2017, v9r1 
    'pp/2017/v9r1/MagUp'   : '/LHCb/Collision17/Beam6500GeV-VeloClosed-MagUp/Real Data/Reco17/Turbo04/PIDCalibTuples9r1/PIDMerge05/95100000/PIDCALIB.ROOT' ,
    'pp/2017/v9r1/MagDown' : '/LHCb/Collision17/Beam6500GeV-VeloClosed-MagDown/Real Data/Reco17/Turbo04/PIDCalibTuples9r1/PIDMerge05/95100000/PIDCALIB.ROOT' ,
    ## 2018, v8r0 
    'pp/2018/v8r0/MagUp'   : '/LHCb/Collision18/Beam6500GeV-VeloClosed-MagUp/Real Data/Reco18/Turbo05/PIDCalibTuples8r0/PIDMerge05/95100000/PIDCALIB.ROOT' ,
    'pp/2018/v8r0/MagDown' : '/LHCb/Collision18/Beam6500GeV-VeloClosed-MagDown/Real Data/Reco18/Turbo05/PIDCalibTuples8r0/PIDMerge05/95100000/PIDCALIB.ROOT' ,
    ## 2018, v9r2 
    'pp/2018/v9r2/MagUp'   : '/LHCb/Collision18/Beam6500GeV-VeloClosed-MagUp/Real Data/Reco18/Turbo05/PIDCalibTuples9r2/PIDMerge05/95100000/PIDCALIB.ROOT' ,
    'pp/2018/v9r2/MagDown' : '/LHCb/Collision18/Beam6500GeV-VeloClosed-MagDown/Real Data/Reco18/Turbo05/PIDCalibTuples9r2/PIDMerge05/95100000/PIDCALIB.ROOT' ,
    #
    ## pA and Ap 
    # 
    'Ap/2016/v5r0/MagDown' : '/LHCb/Ionproton16/Beam6500GeV-VeloClosed-MagDown/Real Data/Reco16pLead/Turbo03pLead/PIDCalibTuples5r0/PIDMerge01/95100000/PIDCALIB.ROOT' ,
    'pA/2016/v5r0/MagDown' : '/LHCb/Protonion16/Beam6500GeV-VeloClosed-MagDown/Real Data/Reco16pLead/Turbo03pLead/PIDCalibTuples5r0/PIDMerge01/95100000/PIDCALIB.ROOT' ,
    ## 
    'Ap/2017/v8r1/MagDown' : '/LHCb/Ionproton16/Beam6500GeV-VeloClosed-MagDown/Real Data/Reco16pLead/Turbo03pLead/PIDCalibTuples5r0/PIDMerge01/95100000/PIDCALIB.ROOT' ,
    'pA/2017/v8r1/MagDown' : '/LHCb/Protonion16/Beam6500GeV-VeloClosed-MagDown/Real Data/Reco16pLead/Turbo03pLead/PIDCalibTuples5r0/PIDMerge01/95100000/PIDCALIB.ROOT' ,
    ## 
    }
    
## elif StripVer == "pATurbo15" or StripVer == "pATurbo16" or StripVer == "ApTurbo15" or StripVer == "ApTurbo16":
##     ############
##     ### 2015 ###
##     ############
##     if year == '15':
##         reco = '15pLead'
##         turbo = '03pLead'
##         version = '5r0'
##         merge = '01'
##     ############
##     ### 2016 ###
##     ############
##     elif year == '16':
##         reco = '16pLead'
##         turbo = '03pLead'
##         version = '5r1'
##         merge = '05'
## bkDict[
##     'ProcessingPass'] = '/Real Data/Reco' + reco + '/Turbo' + turbo + '/PIDCalibTuples' + version + '/PIDMerge' + merge
# 
# =============================================================================
## PIDCALIB data samples
#  https://twiki.cern.ch/twiki/bin/view/LHCbPhysics/ChargedPID
samples  = {
    ## PIDCalibTuples v4r1     
    'v4r1' : {
    'pp/2015/MagDown' : '/eos/lhcb/grid/prod/lhcb/LHCb/Collision15/PIDCALIB.ROOT/00057802/0000/' , 
    'pp/2015/MagUp'   : '/eos/lhcb/grid/prod/lhcb/LHCb/Collision15/PIDCALIB.ROOT/00057792/0000/' , 
    'pp/2016/MagDown' : '/eos/lhcb/grid/prod/lhcb/LHCb/Collision16/PIDCALIB.ROOT/00056408/0000/' , 
    'pp/2016/MagUp'   : '/eos/lhcb/grid/prod/lhcb/LHCb/Collision16/PIDCALIB.ROOT/00056409/0000/' ,         
    },
    ## PIDCalibTuples v5r0 
    'v5r0' : {
    'Ap/2016/MagDown' : '/eos/lhcb/grid/prod/lhcb/LHCb/Ionproton16/PIDCALIB.ROOT/00058288/0000/' , 
    'pA/2016/MagDown' : '/eos/lhcb/grid/prod/lhcb/LHCb/Protonion16/PIDCALIB.ROOT/00058286/0000/' ,
    },
    ## PIDCalibTuples v5r1 
    'v5r1' : {
    'pp/2015/MagDown' : '/eos/lhcb/grid/prod/lhcb/LHCb/Collision15/PIDCALIB.ROOT/00064785/0000/' , 
    'pp/2015/MagUp'   : '/eos/lhcb/grid/prod/lhcb/LHCb/Collision15/PIDCALIB.ROOT/00064787/0000/' ,    
    'pp/2016/MagDown' : '/eos/lhcb/grid/prod/lhcb/LHCb/Collision16/PIDCALIB.ROOT/00064795/0000/' , 
    'pp/2016/MagUp'   : '/eos/lhcb/grid/prod/lhcb/LHCb/Collision16/PIDCALIB.ROOT/00064793/0000/' ,
    },
    ## PIDCalibTuples v8r0 
    'v8r0' : {
    'pp/2018/MagDown' : '/eos/lhcb/grid/prod/lhcb/LHCb/Collision18/PIDCALIB.ROOT/00082949/0000/' , 
    'pp/2018/MagUp'   : '/eos/lhcb/grid/prod/lhcb/LHCb/Collision18/PIDCALIB.ROOT/00082947/0000/' , 
    },     
    ## PIDCalibTuples v8r1 
    'v8r1' : {
    'Ap/2017/MagDown' : '/eos/lhcb/grid/prod/lhcb/LHCb/Ionproton16/PIDCALIB.ROOT/00058288/0000/' , 
    'pA/2017/MagDown' : '/eos/lhcb/grid/prod/lhcb/LHCb/Protonion16/PIDCALIB.ROOT/00058286/0000/' , 
    'pp/2017/MagDown' : '/eos/lhcb/grid/prod/lhcb/LHCb/Collision17/PIDCALIB.ROOT/00090823/0000/' , 
    'pp/2017/MagUp'   : '/eos/lhcb/grid/prod/lhcb/LHCb/Collision17/PIDCALIB.ROOT/00090825/0000/' , 
    },
    ## PIDCalibTuples v9r1
    'v9r1' : {
    'pp/2017/MagDown' : '/eos/lhcb/grid/prod/lhcb/LHCb/Collision17/PIDCALIB.ROOT/00106052/0000/' , 
    'pp/2017/MagUp'   : '/eos/lhcb/grid/prod/lhcb/LHCb/Collision17/PIDCALIB.ROOT/00106050/0000/' , 
    },
    ## PIDCalibTuples v9r2
    'v9r2' : {
    'pp/2018/MagDown' : '/eos/lhcb/grid/prod/lhcb/LHCb/Collision18/PIDCALIB.ROOT/00109278/0000/' , 
    'pp/2018/MagUp'   : '/eos/lhcb/grid/prod/lhcb/LHCb/Collision18/PIDCALIB.ROOT/00109276/0000/' , 
    }, 
    ## PIDCalibTuples v9r3
    'v9r3' : {
    'pp/2016/MagDown' : '/eos/lhcb/grid/prod/lhcb/LHCb/Collision16/PIDCALIB.ROOT/00111825/0000/' , 
    'pp/2016/MagUp'   : '/eos/lhcb/grid/prod/lhcb/LHCb/Collision16/PIDCALIB.ROOT/00111823/0000/' , 
    },
    # =============================================================================================
    ## VB: version is unknown (for me). The paths are taken 2022/08/17 from
    #  https://twiki.cern.ch/twiki/bin/viewauth/LHCbPhysics/ChargedPID#Run_2_samples
    # =============================================================================================
    'vXXX' : {
    ## 2015 
    'pp/2015/MagUp'   : '/eos/lhcb/grid/prod/lhcb/LHCb/Collision15/PIDCALIB.ROOT/00064787/0000/' , 
    'pp/2015/MagDown' : '/eos/lhcb/grid/prod/lhcb/LHCb/Collision15/PIDCALIB.ROOT/00064785/0000/' ,
    ## 2016 all except  Jpsinopt 
    'pp/2016/MagUp'   : '/eos/lhcb/grid/prod/lhcb/LHCb/Collision16/PIDCALIB.ROOT/00111823/0000/' , ## (all except Jpsinopt)    
    'pp/2016/MagDown' : '/eos/lhcb/grid/prod/lhcb/LHCb/Collision16/PIDCALIB.ROOT/00111825/0000/' , ## (all except Jpsinopt)
    ## 2017 all except  Jpsinopt 
    'pp/2017/MagUp'   : '/eos/lhcb/grid/prod/lhcb/LHCb/Collision17/PIDCALIB.ROOT/00106050/0000/' , ## (all except Jpsinopt)    
    'pp/2017/MagDown' : '/eos/lhcb/grid/prod/lhcb/LHCb/Collision17/PIDCALIB.ROOT/00106052/0000/' , ## (all except Jpsinopt)
    ## 2018 all except  Jpsinopt 
    'pp/2018/MagUp'   : '/eos/lhcb/grid/prod/lhcb/LHCb/Collision18/PIDCALIB.ROOT/00109276/0000/' , ## (all except Jpsinopt)
    'pp/2018/MagDown' : '/eos/lhcb/grid/prod/lhcb/LHCb/Collision18/PIDCALIB.ROOT/00109278/0000/' , ## (all except Jpsinopt)
    }, 
    ## 
    'vXXX_nopt' : {
    ## 2016 only Jpsinopt 
    'pp/2016/MagUp'   : '/eos/lhcb/grid/prod/lhcb/LHCb/Collision16/PIDCALIB.ROOT/00111665/0000/' , ## (only Jpsinopt)
    'pp/2016/MagDown' : '/eos/lhcb/grid/prod/lhcb/LHCb/Collision16/PIDCALIB.ROOT/00111667/0000/' , ## (only Jpsinopt)
    ## 2017 only Jpsinopt 
    'pp/2017/MagUp'   : '/eos/lhcb/grid/prod/lhcb/LHCb/Collision17/PIDCALIB.ROOT/00108862/0000/' , ## (only Jpsinopt)    
    'pp/2017/MagDown' : '/eos/lhcb/grid/prod/lhcb/LHCb/Collision17/PIDCALIB.ROOT/00108864/0000/' , ## (only Jpsinopt)
    ## 
    'pp/2018/MagUp'   : '/eos/lhcb/grid/prod/lhcb/LHCb/Collision18/PIDCALIB.ROOT/00108868/0000/' , ## (only Jpsinopt)    
    'pp/2018/MagDown' : '/eos/lhcb/grid/prod/lhcb/LHCb/Collision18/PIDCALIB.ROOT/00108870/0000/' , ## (only Jpsinopt)
    }, 
    }
    

# =============================================================================
## known species of particles 
SPECIES   = ( 'EP'  , 'EM'   ,
              'KP'  , 'KM'   ,
              'MuP' , 'MuM'  , 
              'PiP' , 'PiM'  ,
              'P'   , 'Pbar' )
# =============================================================================
## calibration samples 
PARTICLES = {
    # =========================================================================
    'v4r1' : {
    2015   : {
    'ELECTRONS' : [ 'B_Jpsi_EM', 'B_Jpsi_EP', 'Jpsi_EM', 'Jpsi_EP' ] , 
    'KAONS'     : [ 'DSt3Pi_KM', 'DSt3Pi_KP', 'DSt_KM', 'DSt_KP', 'DsPhi_KM', 'DsPhi_KP', 'Phi_KM', 'Phi_KP' ] ,
    'MUONS'     : [ 'B_Jpsi_MuM', 'B_Jpsi_MuP', 'DsPhi_MuM', 'DsPhi_MuP', 'Jpsi_MuM', 'Jpsi_MuP', 'Phi_MuM', 'Phi_MuP' ] ,
    'PIONS'     : [ 'DSt3Pi_PiM', 'DSt3Pi_PiP', 'DSt_PiM', 'DSt_PiP', 'KS_PiM', 'KS_PiP' ] , 
    'PROTONS'   : [ 'B_Jpsi_P', 'B_Jpsi_Pbar' , 'Jpsi_P', 'Jpsi_Pbar', 'Lam0_HPT_P', 'Lam0_HPT_Pbar',
                    'Lam0_P', 'Lam0_Pbar', 'Lam0_VHPT_P', 'Lam0_VHPT_Pbar', 'LbLcMu_P', 'LbLcMu_Pbar',
                    'LbLcPi_P', 'LbLcPi_Pbar', 'Sigmac0_P', 'Sigmac0_Pbar', 'Sigmacpp_P', 'Sigmacpp_Pbar' ] , 
    },
    2016   : {
    'ELECTRONS' : [ 'B_Jpsi_EM', 'B_Jpsi_EP' ] , 
    'KAONS'     : [ 'DSt_KM', 'DSt_KP', 'DsPhi_KM', 'DsPhi_KP', 'Ds_KM', 'Ds_KP' ] , 
    'MUONS'     : [ 'B_Jpsi_MuM', 'B_Jpsi_MuP', 'DsPhi_MuM', 'DsPhi_MuP', 'Jpsi_MuM', 'Jpsi_MuP' ] , 
    'PIONS'     : [ 'DSt_PiM', 'DSt_PiP', 'KS_PiM', 'KS_PiP' ] , 
    'PROTONS'   : [ 'Lam0_HPT_P', 'Lam0_HPT_Pbar', 'Lam0_P', 'Lam0_Pbar',
                    'Lam0_VHPT_P', 'Lam0_VHPT_Pbar',
                    'LbLcMu_P', 'LbLcMu_Pbar',
                    'LbLcPi_P', 'LbLcPi_Pbar' ] , 
    }} ,
    # =========================================================================
    'v5r0' : {
    2016   : { 
    'KAONS'   : [ 'DSt_KM', 'DSt_KP'     ] , 
    'MUONS'   : [ 'Jpsi_MuM', 'Jpsi_MuP' ] ,
    'PIONS'   : [ 'DSt_PiM', 'DSt_PiP'   ] , 
    'PROTONS' : [ 'Lam0_HPT_P', 'Lam0_HPT_Pbar', 'Lam0_P', 'Lam0_Pbar', 'Lam0_VHPT_P', 'Lam0_VHPT_Pbar' ] ,
    }} ,
    # =========================================================================
    'v5r1' : {
    2015   : {
    'ELECTRONS' : [ 'B_Jpsi_EM', 'B_Jpsi_EP' ] , 
    'KAONS'     : [ 'DSt_KM', 'DSt_KP', 'DsPhi_KM', 'DsPhi_KP'   ] , 
    'MUONS'     : [ 'B_Jpsi_MuM', 'B_Jpsi_MuP', 'Jpsi_MuM', 'Jpsi_MuP', 'Jpsinopt_MuM', 'Jpsinopt_MuP' ], 
    'PIONS'     : [ 'DSt_PiM', 'DSt_PiP', 'KSLL_PiM', 'KSLL_PiP' ] , 
    'PROTONS'   : [ 'Lam0LL_HPT_P' , 'Lam0LL_HPT_Pbar',
                    'Lam0LL_P'     , 'Lam0LL_Pbar'    ,
                    'Lam0LL_VHPT_P', 'Lam0LL_VHPT_Pbar', 'LbLcMu_P', 'LbLcMu_Pbar' ], 
    } ,
    2016   : {
    'ELECTRONS' : [ 'B_Jpsi_EM', 'B_Jpsi_EP' ] , 
    'KAONS'     : [ 'DSt_KM', 'DSt_KP', 'DsPhi_KM', 'DsPhi_KP' ] , 
    'MUONS'     : [ 'B_Jpsi_MuM', 'B_Jpsi_MuP', 'DsPhi_MuM', 'DsPhi_MuP', 'Jpsi_MuM', 'Jpsi_MuP', 'Jpsinopt_MuM', 'Jpsinopt_MuP' ] ,
    'PIONS'     : [ 'DSt_PiM', 'DSt_PiP', 'KSLL_PiM', 'KSLL_PiP' ] ,
    'PROTONS'   : [ 'Lam0LL_HPT_P', 'Lam0LL_HPT_Pbar',
                    'Lam0LL_P', 'Lam0LL_Pbar',
                    'Lam0LL_VHPT_P', 'Lam0LL_VHPT_Pbar', 'LbLcMu_P', 'LbLcMu_Pbar' ] ,
    }},
    # =========================================================================
    'v8r0' : {
    2018   : {
    'KAONS'   : ['DSt_KM', 'DSt_KP', 'DsPhi_KM', 'DsPhi_KP', 'OmegaDDD_KM', 'OmegaDDD_KP', 'OmegaL_KM', 'OmegaL_KP'] , 
    'MUONS'   : ['B_Jpsi_DTF_MuM', 'B_Jpsi_DTF_MuP', 'B_Jpsi_MuM', 'B_Jpsi_MuP',
                 'DsPhi_MuM', 'DsPhi_MuP', 'Jpsi_MuM', 'Jpsi_MuP', 'Jpsinopt_MuM', 'Jpsinopt_MuP'] , 
    'PIONS'   : ['DSt_PiM', 'DSt_PiP', 'KSDD_PiM', 'KSDD_PiP', 'KSLL_PiM', 'KSLL_PiP'] , 
    'PROTONS' : ['Lam0DD_HPT_P', 'Lam0DD_HPT_Pbar', 'Lam0DD_P', 'Lam0DD_Pbar', 'Lam0DD_VHPT_P',
                 'Lam0DD_VHPT_Pbar', 'Lam0LL_HPT_P', 'Lam0LL_HPT_Pbar',
                 'Lam0LL_P', 'Lam0LL_Pbar', 'Lam0LL_VHPT_P', 'Lam0LL_VHPT_Pbar',
                 'LbLcMu_P', 'LbLcMu_Pbar', 'Lc_P', 'Lc_Pbar'] , 
    }} ,
    # =========================================================================
    'v8r1' : {
    2017   : {
    'KAONS'   : ['DSt_KM', 'DSt_KP', 'DsPhi_KM', 'DsPhi_KP', 'OmegaDDD_KM', 'OmegaDDD_KP', 'OmegaL_KM', 'OmegaL_KP'] , 
    'MUONS'   : ['B_Jpsi_DTF_MuM', 'B_Jpsi_DTF_MuP', 'B_Jpsi_MuM', 'B_Jpsi_MuP', 'DsPhi_MuM',
                 'DsPhi_MuP', 'Jpsi_MuM', 'Jpsi_MuP', 'Jpsinopt_MuM', 'Jpsinopt_MuP'  ] , 
    'PIONS'   : ['DSt_PiM', 'DSt_PiP', 'KSDD_PiM', 'KSDD_PiP', 'KSLL_PiM', 'KSLL_PiP' ] , 
    'PROTONS' : ['Lam0DD_HPT_P', 'Lam0DD_HPT_Pbar', 'Lam0DD_P', 'Lam0DD_Pbar',
                 'Lam0DD_VHPT_P', 'Lam0DD_VHPT_Pbar', 'Lam0LL_HPT_P',
                 'Lam0LL_HPT_Pbar', 'Lam0LL_P', 'Lam0LL_Pbar', 'Lam0LL_VHPT_P',
                 'Lam0LL_VHPT_Pbar', 'LbLcMu_P', 'LbLcMu_Pbar', 'Lc_P', 'Lc_Pbar'] , 
    }},
    # =========================================================================
    'v9r1'  : {
    2017      : {
    'KAONS'   : ['DSt_KM', 'DSt_KP', 'DsPhi_KM', 'DsPhi_KP', 'OmegaDDD_KM', 'OmegaDDD_KP', 'OmegaL_KM', 'OmegaL_KP'] , 
    'MUONS'   : ['B_Jpsi_DTF_MuM', 'B_Jpsi_DTF_MuP', 'B_Jpsi_MuM', 'B_Jpsi_MuP', 'DsPhi_MuM',
                 'DsPhi_MuP', 'Jpsi_MuM', 'Jpsi_MuP', 'Jpsinopt_MuM', 'Jpsinopt_MuP'] , 
    'PIONS'   : ['DSt_PiM', 'DSt_PiP', 'KSDD_PiM', 'KSDD_PiP', 'KSLL_PiM', 'KSLL_PiP'] , 
    'PROTONS' : ['Lam0DD_HPT_P', 'Lam0DD_HPT_Pbar', 'Lam0DD_P', 'Lam0DD_Pbar', 'Lam0DD_VHPT_P',
                 'Lam0DD_VHPT_Pbar', 'Lam0LL_HPT_P', 'Lam0LL_HPT_Pbar', 'Lam0LL_P', 'Lam0LL_Pbar',
                 'Lam0LL_VHPT_P', 'Lam0LL_VHPT_Pbar', 'LbLcMu_P', 'LbLcMu_Pbar', 'Lc_P', 'Lc_Pbar'] ,
    }} , 
    # =========================================================================
    'v9r2' : {
    2018   : {
    'KAONS'   : ['DSt_KM', 'DSt_KP', 'DsPhi_KM', 'DsPhi_KP', 'OmegaDDD_KM', 'OmegaDDD_KP', 'OmegaL_KM', 'OmegaL_KP'] , 
    'MUONS'   : ['B_Jpsi_DTF_MuM', 'B_Jpsi_DTF_MuP', 'B_Jpsi_MuM', 'B_Jpsi_MuP', 'DsPhi_MuM', 'DsPhi_MuP', 'Jpsi_MuM', 'Jpsi_MuP'] , 
    'PIONS'   : ['DSt_PiM', 'DSt_PiP', 'KSDD_PiM', 'KSDD_PiP', 'KSLL_PiM', 'KSLL_PiP'] , 
    'PROTONS' : ['Lam0DD_HPT_P', 'Lam0DD_HPT_Pbar', 'Lam0DD_P', 'Lam0DD_Pbar', 'Lam0DD_VHPT_P',
                 'Lam0DD_VHPT_Pbar', 'Lam0LL_HPT_P', 'Lam0LL_HPT_Pbar', 'Lam0LL_P', 'Lam0LL_Pbar',
                 'Lam0LL_VHPT_P', 'Lam0LL_VHPT_Pbar', 'LbLcMu_P', 'LbLcMu_Pbar', 'Lc_P', 'Lc_Pbar'] ,
    }} ,
    # =========================================================================
    'v9r3' : {
    2016   : {
    'KAONS'   : ['DSt_KM', 'DSt_KP', 'DsPhi_KM', 'DsPhi_KP'] ,
    'MUONS'   : ['B_Jpsi_DTF_MuM', 'B_Jpsi_DTF_MuP', 'B_Jpsi_MuM', 'B_Jpsi_MuP', 'DsPhi_MuM', 'DsPhi_MuP', 'Jpsi_MuM', 'Jpsi_MuP'] , 
    'PIONS'   : ['DSt_PiM', 'DSt_PiP', 'KSLL_PiM', 'KSLL_PiP'] , 
    'PROTONS' : ['Lam0LL_HPT_P', 'Lam0LL_HPT_Pbar', 'Lam0LL_P', 'Lam0LL_Pbar',
                 'Lam0LL_VHPT_P', 'Lam0LL_VHPT_Pbar', 'LbLcMu_P', 'LbLcMu_Pbar', 'Lc_P', 'Lc_Pbar'] , 
    }} ,
    # =========================================================================
    'vXXX'   : {
    2015   : {
    'KAONS'   : ['DSt_KM', 'DSt_KP', 'DsPhi_KM', 'DsPhi_KP'] ,
    'MUONS'   : ['B_Jpsi_MuM', 'B_Jpsi_MuP', 'Jpsi_MuM', 'Jpsi_MuP', 'Jpsinopt_MuM', 'Jpsinopt_MuP' ] ,
    'PIONS'   : ['DSt_PiM', 'DSt_PiP', 'KSLL_PiM', 'KSLL_PiP'] , 
    'PROTONS' : ['Lam0LL_HPT_P', 'Lam0LL_HPT_Pbar', 'Lam0LL_P', 'Lam0LL_Pbar', 'Lam0LL_VHPT_P', 'Lam0LL_VHPT_Pbar', 'LbLcMu_P', 'LbLcMu_Pbar'] 
    } ,
    2016   : {
    'KAONS'   : ['DSt_KM', 'DSt_KP', 'DsPhi_KM', 'DsPhi_KP'] ,
    'MUONS'   : ['B_Jpsi_DTF_MuM', 'B_Jpsi_DTF_MuP', 'B_Jpsi_MuM', 'B_Jpsi_MuP', 'DsPhi_MuM', 'DsPhi_MuP', 'Jpsi_MuM', 'Jpsi_MuP'],
    'PIONS'   : ['DSt_PiM', 'DSt_PiP', 'KSLL_PiM', 'KSLL_PiP'] ,                                                                                                    
    'PROTONS' : ['Lam0LL_HPT_P', 'Lam0LL_HPT_Pbar', 'Lam0LL_P', 'Lam0LL_Pbar', 'Lam0LL_VHPT_P', 'Lam0LL_VHPT_Pbar', 'LbLcMu_P', 'LbLcMu_Pbar', 'Lc_P', 'Lc_Pbar']
    } ,
    2017   : {
    'KAONS'   : ['DSt_KM', 'DSt_KP', 'DsPhi_KM', 'DsPhi_KP', 'OmegaDDD_KM', 'OmegaDDD_KP', 'OmegaL_KM', 'OmegaL_KP'] ,
    'MUONS'   : ['B_Jpsi_DTF_MuM', 'B_Jpsi_DTF_MuP', 'B_Jpsi_MuM', 'B_Jpsi_MuP', 'DsPhi_MuM', 'DsPhi_MuP', 'Jpsi_MuM', 'Jpsi_MuP'] ,
    'PIONS'   : ['DSt_PiM', 'DSt_PiP', 'KSDD_PiM', 'KSDD_PiP', 'KSLL_PiM', 'KSLL_PiP'] ,
    'PROTONS' : ['Lam0DD_HPT_P', 'Lam0DD_HPT_Pbar', 'Lam0DD_P', 'Lam0DD_Pbar', 'Lam0DD_VHPT_P', 'Lam0DD_VHPT_Pbar', 'Lam0LL_HPT_P', 'Lam0LL_HPT_Pbar', 'Lam0LL_P', 'Lam0LL_Pbar', 'Lam0LL_VHPT_P', 'Lam0LL_VHPT_Pbar', 'LbLcMu_P', 'LbLcMu_Pbar', 'Lc_P', 'Lc_Pbar']
    },
    2018   : {
    'KAONS'   : ['DSt_KM', 'DSt_KP', 'DsPhi_KM', 'DsPhi_KP', 'OmegaDDD_KM', 'OmegaDDD_KP', 'OmegaL_KM', 'OmegaL_KP'] ,                                                                                                                                                
    'MUONS'   : ['B_Jpsi_DTF_MuM', 'B_Jpsi_DTF_MuP', 'B_Jpsi_MuM', 'B_Jpsi_MuP', 'DsPhi_MuM', 'DsPhi_MuP', 'Jpsi_MuM', 'Jpsi_MuP'] ,
    'PIONS'   : ['DSt_PiM', 'DSt_PiP', 'KSDD_PiM', 'KSDD_PiP', 'KSLL_PiM', 'KSLL_PiP'] , 
    'PROTONS' : ['Lam0DD_HPT_P', 'Lam0DD_HPT_Pbar', 'Lam0DD_P', 'Lam0DD_Pbar', 'Lam0DD_VHPT_P', 'Lam0DD_VHPT_Pbar', 'Lam0LL_HPT_P', 'Lam0LL_HPT_Pbar', 'Lam0LL_P', 'Lam0LL_Pbar', 'Lam0LL_VHPT_P', 'Lam0LL_VHPT_Pbar', 'LbLcMu_P', 'LbLcMu_Pbar', 'Lc_P', 'Lc_Pbar']
    } ,
    } , 
    'vXXX_nopt' : {
    2016     : {
    'MUONS'  : ['Jpsinopt_MuM', 'Jpsinopt_MuP'] 
    } ,
    2017     : {
    'MUONS'  : ['Jpsinopt_MuM', 'Jpsinopt_MuP'] 
    } ,
    2018     : {
    'MUONS'  : ['Jpsinopt_MuM', 'Jpsinopt_MuP'] 
    } ,
    } ,
    }

# =============================================================================
## TTree names
electrons = set()
kaons = set()
muons = set()
pions = set()
protons = set()
for v in PARTICLES:
    for y in PARTICLES [ v ]:
        electrons |= set ( PARTICLES [ v ] [ y ] . get ( 'ELECTRONS' , [] ) )
        kaons     |= set ( PARTICLES [ v ] [ y ] . get ( 'KAONS'     , [] ) )
        muons     |= set ( PARTICLES [ v ] [ y ] . get ( 'MUONS'     , [] ) )
        pions     |= set ( PARTICLES [ v ] [ y ] . get ( 'PIONS'     , [] ) )
        protons   |= set ( PARTICLES [ v ] [ y ] . get ( 'PROTONS'   , [] ) )
 
e_plus      = tuple ( electrons )
e_minus     = tuple ( e.replace ('_EP', '_EM'  ) for e in e_plus     )
kaons_plus  = tuple ( kaons     )
kaons_minus = tuple ( k.replace ('_KP', '_KM'  ) for k in kaons_plus )
muons_plus  = tuple ( muons     )
muons_minus = tuple ( m.replace ('_MuP', '_MuM') for m in muons_plus )
pions_plus  = tuple ( pions     )
pions_minus = tuple ( p.replace ('_PiP', '_PiM') for p in pions_plus )
protons     = tuple ( protons   )
antiprotons = tuple ( p + 'bar' for p in protons)

for v in PARTICLES:
    for y in PARTICLES [ v ] :
        for k in PARTICLES [ v ] [ y ] :
            ps = set ( PARTICLES [ v ] [ y ] [ k ] )
            if   'ELECTRONS' == k : ps |= set ( p.replace ( '_EP'  , '_EM'  ) for p in ps )
            elif 'KAONS'     == k : ps |= set ( p.replace ( '_KP'  , '_KM'  ) for p in ps )
            elif 'MUONS'     == k : ps |= set ( p.replace ( '_MuP' , '_MuM' ) for p in ps )
            elif 'PIONS'     == k : ps |= set ( p.replace ( '_PiP' , '_PiM' ) for p in ps )
            elif 'PROTONS'   == k :
                ps |= set ( p.replace ( '_P' , '_Pbar' ) for p in ps if not '_Pbar' in p )
            PARTICLES [ v ] [ y ] [ k ] = tuple ( ps )


# =============================================================================
## prepare the parser
def make_parser():
    """ Prepare the parser
    - oversimplified version of parser from MakePerfHistsRunRange.py script
    """
    import argparse, os, sys

    parser = argparse.ArgumentParser (
        formatter_class = argparse.RawDescriptionHelpFormatter,
        prog            = os.path.basename ( sys.argv[0] ) ,
        description     = """Make performance histograms for a given:
        a) data taking period <YEAR>        ( e.g. 2015    )
        b) magnet polarity    <MAGNET>      ( 'MagUp' or 'MagDown' or 'Both' )
        c) particle type      <PARTICLE>    ( 'K', 'P' , 'Pi' , 'e' , 'Mu'   )
        """ + '\n' + __doc__ + '\n\n\n',
    )

    ## add mandatory arguments
    parser.add_argument (
        'particle'    ,
        metavar = '<PARTICLE>'    , type=str    ,
        choices = \
        ( 'p'  , 'p+'  , 'p-'  ) + ( 'P'  , 'P+'  , 'P-'  ) + protons    + antiprotons + \
        ( 'K'  , 'K+'  , 'K-'  ) + ( 'k'  , 'k+'  , 'k-'  ) + kaons_plus + kaons_minus + \
        ( 'pi' , 'pi+' , 'pi-' ) + ( 'Pi' , 'Pi+' , 'Pi-' ) +
        ( 'PI' , 'PI+' , 'PI-' ) + pions_plus + pions_minus + \
        ( 'e'  , 'e+'  , 'e-'  ) + ( 'E'  , 'E+'  , 'E-'  ) + e_plus     + e_minus     + \
        ( 'mu' , 'mu+' , 'mu-' ) + ( 'Mu' , 'Mu+' , 'Mu-' ) +
        ( 'MU' , 'MU+' , 'MU-' ) + muons_plus + muons_minus ,
        help    = "Sets the particle type"     )

    parser.add_argument(
        '-y',
        '--years',
        metavar  = '<YEARS>'   ,
        default = []           ,
        type    = int          , 
        nargs   = '+'          ,
        help    = "Data taking periods to process")

    parser.add_argument(
        '-x'            ,
        '--collisions'  ,
        default       = 'pp'           ,
        metavar       = '<COLLISIONS>' ,
        type          = str            ,
        choices       = ( 'pp' ,  'pA' ,  'Ap' ) ,
        help          = "Collision type" )

    ## add the optional arguments
    parser.add_argument(
        '-p'                       ,
        '--polarity'               ,
        default       = 'Both'     ,
        metavar       = '<MAGNET>' ,
        type          = str        ,
        choices       = ( 'MagUp'  ,  'MagDown' ,  'Both' ) ,
        help          ="Magnet polarity")

    parser.add_argument(
        '-f'             ,
        '--maxfiles'     ,
        dest          = "MaxFiles" ,
        metavar       = "<NUM>"    ,
        type          = int        ,
        default       = -1         ,
        help          = "The maximum number of calibration files per configuration to process")

    parser.add_argument(
        '-c'                       ,
        '--cuts'                   ,
        dest           = 'cuts'    ,
        metavar        = '<CUTS>'  ,
        default        = ''        ,
        help           = """Additional cuts to apply to the calibration sample
        prior to determine the PID efficiencies,
        e.g. fiducial volume,  HASRICH, etc...
        """)
    
    parser.add_argument(
        '-o'   ,
        '--output'                  ,
        type       = str            ,
        default    = 'PIDCALIB2.db' ,
        help       = "The name of output database file")

    parser.add_argument(
        '-v'                   ,
        '--versions'           ,
        default = []           ,
        metavar = '<VERSIONS>' ,
        nargs   = '*'          ,
        help    = "Versions of PIDCalibTuples to be used")
    
    parser.add_argument(
        '-s'                       ,
        '--samples'                ,
        default      = []          ,
        dest         = 'Samples'   ,
        metavar      = '<SAMPLES>' ,
        nargs        = '*'         ,
        help         = 'The (test) samples to be processed')

    parser.add_argument(
        '-r'                       ,
        '--regex'                  ,
        default      = ''          ,
        type         = str         ,
        dest         = 'Regex'     ,
        metavar      = '<REGEX>'   ,
        help         = 'Process only calibration samples that match this regular expression')
    
    tests = parser.add_mutually_exclusive_group()
    tests.add_argument(
       '--testfiles'                  ,
        default       = ''            , 
        dest          = 'TestFiles'   ,
        metavar       = '<TESTFILES>' ,
        type          =  str          ,
        help          = 'The pattern to load test-files (year, polarity, etc are ignored)'
    )
    tests.add_argument(
        '--testpath'                ,
        dest         ='TestPath'    ,
        metavar      = '<TESTPATH>' ,
        type         = str          ,
        default      = ''           ,
        help         = 'The path in DB (year, polarity, etc are ignored) to load test data' )

    addGroup = parser.add_argument_group("further options")
    addGroup.add_argument(
        "-q"                     ,
        "--quiet"                ,
        dest    = "verbose"      ,
        action  = "store_false"  ,
        default = True           ,
        help    = "Suppresses the printing of verbose information" )
    
    addGroup.add_argument(
        "-e"                   ,
        "--useeos"             ,
        dest    = "UseEos"     ,
        action  = "store_true" ,
        default = False        ,
        help    = "For GRID files use them from local eos, if possible.")
    addGroup.add_argument(
        "-z"                   ,
        "--parallel"           ,
        dest    = "Parallel"   ,
        action  = "store_true" ,
        default = False        ,
        help    = "Use parallelization (does not work with GRID files,``UseEos'' is needed)" )
    
    addGroup.add_argument (
        "-u"   ,
        "--use-frame"                 , 
        dest    = "UseFrame"          ,
        action  = "store_true"        , 
        default = False               , ## (6,25) <= root_info ,
        help    = "Use efficient DataFrame for processing (for ROOT>6.25)" ) 
    addGroup.add_argument(
        "-d"                   ,
        "--dump"               ,
        dest    = "Dump"       ,
        action  = "store_true" ,
        default = False        ,
        help    = "Add useful statistics to the output" )
    
    addGroup.add_argument (
        "-g"     ,
        "--grid" ,
        action   = "store_true" , 
        default  = False        ,
        dest     = 'UseGrid'    ,
        help     = "Use GRID to get data (add ``UseEos'' is want to process in parallel)"
        )
    addGroup.add_argument (
        "-b"      ,
        "--batch" ,
        action    = "store_true" , 
        default   = False        ,
        dest      = 'Batch'      ,
        help      = "Batch processing (do not show the plots)"
        )
    addGroup.add_argument (
        "--chunk" ,
        type      = int          , 
        default   = 20           ,
        dest      = 'ChunkSize'  ,
        help      = "Chunk size for the parallel processing"
        )
    
    return parser


# =============================================================================
## load certain calibration files  using given file patterns
def load_data ( pattern           ,
                particles         ,
                tag       = ''    ,
                maxfiles  = -1    ,
                verbose   = True  ,
                data      = {}    ,
                what      = '...' ) :
    """Load certain calibration files  using given file patterns
    """

    from ostap.trees.data import Data

    logger.info ( 'Loading data %s ' % what  )
    loaded = 0 
    for i , p in enumerate ( progress_bar ( tuple ( particles ) ) ) : 
        chain  = p + 'Tuple/DecayTree'
        d      = Data ( chain , pattern , maxfiles = maxfiles , silent = True , check = False )
        key    = '%s/%s' % ( tag , p )

        logger.debug ( 'Loaded data [%2d/%-2d] for key %s: %s' % ( i , len ( particles ) , key , d ) )        
        if not d:
            logger.warning ( 'No useful data is found for %s' % key )
            continue

        data [ key ] = d
        loaded += len ( d.files )
        
    return data


# =============================================================================
## Load calibration samples
def load_samples ( particles,
                   years      = ( '2015', '2016','2017','2018') ,
                   collisions = ( 'pp'  , 'pA'  , 'Ap' )        , 
                   polarity   = ( 'MagDown' , 'MagUp'  )        ,
                   versions   = [ 'v5r1' ] ,
                   grid       = True  ,  
                   maxfiles   = -1    ,                   
                   verbose    = False ,
                   use_eos    = False ):
    """Load calibration samples
    """

    if grid : 
        try:
            from pidcalib.grid import hasGridProxy
            if hasGridProxy():
                return load_samples_from_grid ( particles  = particles  ,
                                                years      = years      ,
                                                collisions = collisions ,
                                                polarity   = polarity   ,
                                                versions   = versions   ,
                                                maxfiles   = maxfiles   ,
                                                verbose    = verbose    ,
                                                use_eos    = use_eos    )
            logger.error ("No grid proxy, switch off to local look-up")            
        except ImportError:
            logger.warning("Cannot import ``grid'', switch off to local look-up")
            
    if 0 < maxfiles:
        logger.warning('Only max=%d files per configuration will be processed!' % maxfiles)

    maxfiles = maxfiles if 0 < maxfiles else 1000000
    
    data     = {}
    for y in years:
        for c in collisions:
            for p in polarity:
                tag = '%s/%s/%s' % (c, y, p)                
                for version in versions :                    
                    if not version in samples : continue
                    
                    fdir = samples[version].get(tag, None)
                    if not fdir:
                        logger.warning ( 'No data is found for Collisions="%s" , Year="%s" , Polarity="%s", Version="%s"' % (c, y, p, version ) )
                        continue

                    if not os.path.exists ( fdir ) :
                        logger.error ( 'Non-existing directory    : %s, skip!' )
                        continue
                    if not os.path.isdir ( fdir ) :
                        logger.error ( 'The path is not directory : %s, skip!' )
                        continue

                    ## file pattern:
                    pattern = os.path.join ( fdir, '*.pidcalib.root' ) 
                    
                    ## load files
                    what = 'Collisions=%s, Year=%s, Polarity=%s, Version=%s' % ( c , y, p , version )
                    new_data = load_data ( pattern     ,
                                           particles   ,
                                           tag         ,
                                           maxfiles    ,
                                           verbose     ,
                                           data        ,
                                           what = what )
                    data.update ( new_data )
                    del new_data

    return data


# =============================================================================
## Load calibration samples
def load_samples_from_grid ( particles  ,
                             years      =  ( '2015' , '2016' , '2017' , '2028' ) ,
                             collisions = ('pp'   , 'pA'   , 'Ap'),
                             polarity   = ('MagDown', 'MagUp') ,
                             versions   = [ 'v5r1' ] ,
                             maxfiles   = -1    ,
                             verbose    = False ,
                             use_eos    = False ) :
    """Load calibration samples from GRID
    """

    the_path = '/LHCb/{collision}{year}/Beam6500GeV-VeloClosed-{magnet}/Real Data/Reco{reco}/Turbo{turbo}/PIDCalibTuples{version}/PIDMerge{merge}/95100000/PIDCALIB.ROOT'

    problems = set() 
    maxfiles = maxfiles if 0 < maxfiles else 1000000
    data     = {}
    for year in years:
        for c in collisions:
            for magnet in polarity:
                
                for version in versions :
                    
                    key = '%s/%s/%s/%s' % ( c, year, version, magnet)
                
                    path = bookkeeping_paths.get ( key , '' )
                    if not path:
                        problems.add ( key ) 
                        continue

                    new_data = load_from_grid (
                        path                ,
                        particles           ,
                        tag      = key      ,
                        maxfiles = maxfiles ,
                        verbose  = verbose  ,
                        use_eos  = use_eos  )
                    
                    data.update ( new_data )
                    del new_data

    if problems :
        problems = tuple ( sorted ( problems ) )
        for p in problems : 
            logger.warning("Cannot find bookeeping entry for %s" % p)
        
    return data


# =============================================================================
## Load calibration samples
def load_from_grid ( path             ,
                     particles        ,
                     tag      = ''    ,
                     maxfiles = -1    ,
                     verbose  = False ,
                     use_eos  = False ):
    """Load calibration samples from GRID
    """

    try:
        from pidcalib.grid import BKRequest, filesFromBK
    except ImportError:
        logger.error("Can't import from ``pidcalib.grid''")
        return {}

    logger.debug('Make a try with path "%s"' % path)

    data = {}

    request  = BKRequest ( path      = path     ,
                           nmax      = maxfiles ,
                           accessURL = True     )  ## , SEs = 'CERN-DST-EOS' )
    files    = filesFromBK ( request )
    
    # =========================================================================
    if use_eos : 
        files_ = []
        ## eos_   = 'root://eoslhcb.cern.ch/'
        eos_tag   = '/eos/lhcb/grid/'
        skip  = set() 
        for f in files :
            p = f.find ( eos_tag )
            if 0 < p :
                ff = f[p:] 
                if os.path.exists ( ff ) and os.path.isfile ( ff ) and os.access ( ff , os.R_OK ) :
                    with ROOT.TFile.Open ( ff , 'r' , exception = False ) as rfile :
                        if rfile and rfile.IsOpen () :
                            files_.append ( ff )
                        else : skip.add  ( f )                         
                else :
                    skip.add  ( f ) 

        files_.sort()
        files = files_
        
        if skip :
            logger.warning ("Cannot find eos replica for %s files" % len ( skip ) )
            for s in sorted ( skip ) : logger.warning ( 'File skept %s' % s )
                
    # =========================================================================
    
    dirs     = set ()
    for f in files:
        i1 = f.find('/eos/')
        if 0 <= i1:
            i2 = f.find('/0000/', i1)
            if i1 < i2: dirs.add(f[i1:i2] + '/0000/')

    if not files:
        logger.warning("Can't get data from path \"%s\"" % path)
        request  = BKRequest (  path = path , nmax = maxfiles , accessURL=False )  ## , SEs = 'CERN-EOS' )
        files    = filesFromBK ( request )
        if not files: logger.error("Can't get data from path \"%s\"" % path)
        files = [ 'root://eoslhcb.cern.ch//eos/lhcb/grid/prod' + f for f in files ]

    ## load files
    if files:
        logger.info ( 'Got %d files from "%s"' % ( len ( files ) , path ) )
        ## if verbose and dirs: logger.info('EOS-directories: %s' % list ( dirs ) )
        if not tag: tag = path
        data = load_data ( files , particles , tag , maxfiles , verbose , data , what = path )

    if verbose and data and dirs :
        logger.info('EOS directories: %s ' % list ( dirs ) )

    return data


# =============================================================================
## Helper get EOS directories for GRID paths
#  - It is useful to populate <code>samples<code> from <code>bookkeeping_paths</code>
def get_eos_dirs ( silent = False )  :
    """Get EOS directories for GRID paths
    - It is useful to populate `samples` from `bookkeeping_paths`
    """
    
    try : 
        from pidcalib.grid import BKRequest, filesFromBK, hasGridProxy 
    except ImportError:
        logger.error("Can't import from ``pidcalib.grid''")
        return {}

    if not hasGridProxy () :
        logger.error ('get_eos_dirs: no grid proxy!')
        return {} 

    result = {} 
    for key in progress_bar ( sorted ( bookkeeping_paths ) , silent = silent ) :
        path = bookkeeping_paths [ key ]
        
        data = {}
        
        request  = BKRequest ( path      = path  ,
                               nmax      = -1    ,
                               accessURL = True  ) ## , SEs = 'CERN-DST-EOS' )
        files    = filesFromBK ( request )
        
    
        files_ = []
        eos_   = '/eos/lhcb/grid/prod/lhcb/'
        for f in files :
            p = f.find ( eos_ )
            if 0 <= p : 
                ff = f [ p: ]
                if os.path.exists ( ff ) and os.path.isfile ( ff ) and os.access ( ff , os.R_OK ) :
                    files_.append ( ff )
                    continue
                files_.append ( f )        
        files = files_
                
        dirs     = set ()
        for f in files:
            i1 = f.find('/eos/')
            if 0 <= i1:
                i2 = f.find('/0000/', i1)
                if i1 < i2: dirs.add(f[i1:i2] + '/0000/')
                
        dirs = list ( dirs )
        
        result [ key ] = tuple ( sorted ( dirs ) )

    return result 

# =============================================================================
## get available samples from the list of files 
def get_samples ( files , silent = False , nmax = -1 ) :
    """Get available samples from the list of files 
    """
    if isinstance ( files , str ) : files = [ files ]
    
    from collections import defaultdict
    res  = defaultdict(set)

    strange = set()

    if 0 < nmax : 
        files   = [ f for f in files ]
        if nmax < len ( files ) : files = files [ : nmax ] 
    
    for fname  in progress_bar ( files , silnet = silent ) :
        ##
        f = ROOT.TFile.Open ( fname , 'READ' )
        if not f : continue
        keys = f.keys()
        for key in sorted ( keys ) :
            if not key.endswith('Tuple/DecayTree')  : continue
            tag = key.replace ( 'Tuple/DecayTree' , '' )
            #
            if   tag.endswith ( '_KP'  ) or tag.endswith ( '_KM'   ) : res [ 'KAONS'     ].add ( tag )  
            elif tag.endswith ( '_PiP' ) or tag.endswith ( '_PiM'  ) : res [ 'PIONS'     ].add ( tag )  
            elif tag.endswith ( '_MuP' ) or tag.endswith ( '_MuM'  ) : res [ 'MUONS'     ].add ( tag )  
            elif tag.endswith ( '_P'   ) or tag.endswith ( '_Pbar' ) : res [ 'PROTONS'   ].add ( tag )  
            elif tag.endswith ( '_EP'  ) or tag.endswith ( '_EM'   ) : res [ 'ELECTRONS' ].add ( tag )  
            else :
                strange.add ( tag ) 

    result = {}
    for k in sorted ( res ) :
        lst = list ( res[k] )
        if not lst : continue
        result [ k ]  = tuple ( sorted ( lst ) ) 
        
    strange = list ( strange )
    strange = tuple ( sorted ( strange ) ) 
    return result, strange 


# =============================================================================
def dump_samples () :

    import glob

    for vers in sorted ( samples ) :
        
        sets = samples [ vers ]
        
        for key in sorted ( sets ) :

            sample = sets [ key ]
            pattern = '%s*pidcalib.root' % sample

            regular, strange = get_samples ( glob.iglob ( pattern ) , nmax = 5 )

            logger.info ( 'Regular Samples for %s %s' % ( key , vers ) ) 
            
            for p in sorted ( regular  ) :
                logger.info ( '%10s : %s' % ( p , list ( regular [p] ) ) ) 
                
            if strange :
                logger.warning ('Strange samples:%s' % list ( strange ) )
                
# =============================================================================
## Run PID-calib machinery
def run_pid_calib(FUNC, args=[]):
    """ Run PID-calib procedure
    """

    import sys
    vargs = args + [a for a in sys.argv[1:] if '--' != a]

    parser = make_parser()
    config = parser.parse_args ( vargs )

    message  = 'PIDCalib2: processing of PIDCalib samples for Run-II'
    if config.verbose : message = '\n'.join ( [ message , __doc__ ] ) 
    logger.info ( message ) 
    
    good_years = ( 2015 , 2016 , 2017 , 2018 ) 
    if not all (  ( y in good_years )  for y in config.years ) :
        parser.exit ( message = "Invalid ``YEARS'': %s" % str ( config.years ) ) 

    ## config.UseFrame = False
    if config.UseFrame and not (6,25) <= root_info :
        config.UseFrame = False
        logger.warning('Processing via DataFrame is disabled!')
    
        
    ## if config.Parallel :
    ##     if not config.UseEos :
    ##         logger.warning("Parallel processing is disabled (due to ``UseEos'' setting)")
    ##         config.Parallel = False
            
    if config.Parallel and (3,6) <= sys.version_info :
        
        try :
            import dill
            dill_version = dill.__version__ 
        except:
            dill_version = None
            dill = None
            
        if   dill and '0.3.3' <= dill_version : pass
        elif not dill                         : pass
        else :
            logger.warning("Parallel processing is disabled (due to dill/python/ROOT issue)")
            config.Parallel = False

    table = [ ( 'Parameter' , 'Value' ) ]
    conf  = vars ( config )
    for k in sorted ( conf ) :
        row = k , str ( conf [ k ] )
        table.append ( row ) 
    import ostap.logger.table as T
    table = T.table ( table , title = 'Parser configuration', prefix = '# ' , alignment = 'rw' , indent = '' )
    logger.info ( 'Parser configuration\n%s' % table ) 
 
    if config.TestPath:
        logger.warning ( 'TestPath:  Year/Polarity/Collision/Version will be ignored' )
    elif config.TestFiles:
        logger.warning ( 'TestFiles: Year/Polarity/Collision/Version will be ignored' )
       
    if config.Parallel : logger.info ('Parallel processing     is activated!')
    if config.UseFrame : logger.info ('Procesing via DataFrame is activated!') 
    if config.UseFrame and not ROOT.ROOT.IsImplicitMTEnabled() :
        ROOT.ROOT.EnableImplicitMT  ()
        logger.info ( "Implicit MT is enabled" ) 

    if config.Batch : ROOT.gROOT.SetBatch ( True )
    if ROOT.gROOT.IsBatch() : logger.info ( "ROOT is in 'batch' mode" )
    
    return pid_calib ( FUNC , config)


# =============================================================================
## Get the staticstics for the tree/chain for given variables 
def get_statistics ( chain , variables                    ,
                     use_frame =  ( 6 , 25 ) <= root_info ,
                     parallel  =  False                   ) :
    """Get the statistics for the chain for given variables
    """
    if ( 6 , 25 ) <= root_info and use_frame :
        
        from ostap.frames.frames import DataFrame, frame_statVars             
        frame = DataFrame ( chain ) ## , enable = True ) 
        return frame_statVars ( frame , variables, lazy = False )

    if parallel :  stats = chain.pstatVars ( variables )
    else        :  stats = chain. statVars ( variables )
    
    for k in stats :
        s = stats [ k ] 
        stats [ k ] = s.values()
        
    return stats

            
# =============================================================================


from ostap.parallel.task import   Task
# =============================================================================
## @class PidCalibTask
#  Task for the parallel processing 
class PidCalibTask(Task) :
    """Task for the parallel processing"""
    def __init__ ( self , pidfunc ) :
        self.__pidfunc  = pidfunc
        self.__output   = {}
    def initialize_local  ( self              ) : self.__output = {}
    def initialize_remote ( self , jobid = -1 ) : self.__output = {}
    ## actual processing 
    def process ( self , jobid , item ) :
        """Actual processing"""

        ## unpack configuration
        key , name , files , use_frame , _  = item 

        import ROOT
        import ostap.core.pyrouts        
        import ostap.io.root_file

        data = ROOT.TChain  ( name )
        for f in files : data.Add ( f )

        ## the actual processing 
        self.__output = {  key : self.__pidfunc.run ( data , use_frame = use_frame , parallel = False ) }

        del data        
        return self.__output 

    ## get the results 
    def results (  self ) : return self.__output 

    ## merge the results 
    def merge_results  ( self , results , jobid = -1 ) :

        while results :
            key , item  = results.popitem()
            a   , r     = item
            if key in self.__output :
                sa , sr = self.__output [ key ]
                sa.Add ( a ) 
                sr.Add ( r ) 
                self.__output [ key ] = sa , sr
                del a
                del r 
            else  :
                self.__output [ key ] =  a ,  r 


# =============================================================================
## The simple task object collect statistics for loooooong chains 
#  @author Vanya BELYAEV Ivan.Belyaev@itep.ru
#  @date   2014-09-23
class StatVar2Task(Task) :
    """The simple task object collect statistics for loooooong chains 
    """
    ## constructor: histogram 
    def __init__ ( self , what , cuts = '' ) :
        """Constructor        
        >>> task  = StatVar2Task ( ('mass','pt') , 'pt>1') 
        """
        self.what     = what 
        self.cuts     = str(cuts) 
        self.__output = {}
        
    ## local initialization (executed once in parent process)
    def initialize_local   ( self ) :
        """Local initialization (executed once in parent process)
        """
        from ostap.stats.counters import WSE
        self.__output = {}
            
    ## the actual processing
    #   ``params'' is assumed to be a tuple/list :
    #  - the file name
    #  - the tree name in the file
    #  - the variable/expression/expression list of quantities to project
    #  - the selection/weighting criteria 
    #  - the first entry in tree to process
    #  - number of entries to process
    def process ( self , jobid , item ) :
        """The actual processing
        ``params'' is assumed to be a tuple-like entity:
        - the file name
        - the tree name in the file
        - the variable/expression/expression list of quantities to project
        - the selection/weighting criteria 
        - the first entry in tree to process
        - number of entries to process
        """

        import ROOT
        from ostap.logger.utils import logWarning
        with logWarning() :
            import ostap.core.pyrouts 
            import ostap.trees.trees 
            
        ## unpack configuration
        key , name , files = item
        
        data = ROOT.TChain  ( name )
        for f in files : data.Add ( f )
        
        from ostap.trees.trees  import _stat_vars_

        self.__output = { key : _stat_vars_ ( data , self.what , self.cuts ) } 

        del data        
        return self.__output 
        
    ## get the results 
    def results ( self ) : return self.__output 

    ## merge results 
    def merge_results ( self , result , jobid = -1 ) :
        
        from ostap.stats.counters import WSE
        
        if not self.__output : self.__output = result
        else               :
            assert type( self.__output ) == type ( result ) , 'Invalid types for merging!'
            
            while result :
                key , rstat = result.popitem()

                if key in self.__output :
                    
                    ostat = self.__output [  key ]
                
                    for k in rstat :
                        
                        if k in ostat : ostat [ k ] += rstat [ k ]
                        else          : ostat [ k ]  = rstat [ k ] 
                        
                else :
                    
                    self.__output [ key ] = rstat 
                    

# =============================================================================
## Check the presence of claibration samples in the files
#  @attention there is a sequential loop over the files
def check_samples ( files ) :
    """ Check the presence of claibration samples in the files
    -attention there is a sequential loop over the files
    """
    from collections import defaultdict
    sk    = defaultdict ( list )
    
    other = '*OTHER*'

    ## all calibration species 
    ss = set()
    
    with timing ( 'Check for calibration samples... (sequential loop over %d files)' % len ( files ) , logger = logger ) : 
        for f in progress_bar ( files ) :
            with ROOT.TFile.Open ( f , 'READ' ) as ff :
                keys = ff.keys()
                for k in keys:
                    p = k.find ( '/DecayTree' )
                    if 0 < p: ss.add ( k [ : p ] )
                    
    for s in ss:
        s = s.replace ( 'Tuple' , '' )
        a, b, q = s.rpartition('_')
        if   q in ( 'PiP' , 'PiM' ) : sk [ 'PIONS'    ] . append ( s )
        elif q in ( 'MuP' , 'MuM' ) : sk [ 'MUONS'    ] . append ( s )
        elif q in ( 'KP'  , 'KM'  ) : sk [ 'KAONS'    ] . append ( s )
        elif q in ( 'P'   , 'Pbar') : sk [ 'PROTONS'  ] . append ( s )
        elif q in ( 'EP'  , 'EM'  ) : sk [ 'ELECTRONS'] . append ( s )
        else:
            sk [ other ] . append ( s )

    for k in sk : sk [ k ] . sort ()
    
    table = [ ( 'Particle' , '#' , 'Samples' )  ]
    for k in sorted ( sk ) :
        row = k , '%d' % len ( sk [ k ] ) , str ( sk [ k ] ) 
        table.append ( row )
        
    import ostap.logger.table as T
    
    title = 'Found %2d calibration samples' % len ( ss )
    table = T.table ( table , title = title , prefix = '# ' , alignment = 'lrw' )
    logger.info ( '%s\n%s' % ( title , table ) )  

    keys   = sk.keys()
    smpls1 = defaultdict ( int )
    smpls2 = defaultdict ( int )
    
    #
    ## ALL samples in the found files
    # 
    with timing ( 'Check for PIDCalib samples... (sequential loop over %s files)' % len ( files ) , logger =  logger )  :
        for f in progress_bar ( files ) :
            with ROOT.TFile.Open ( f , 'READ' ) as ff : 
                for k in sorted ( sk )  :
                    sk[k].sort()
                    for s in sk[k] : 
                        c = ff.get ( s + 'Tuple/DecayTree', None )
                        if not c : continue
                        smpls1 [ s ] += len ( c ) 
                        smpls2 [ s ] += 1 
                            
    table = [ ( 'Sample' , '#files' , '#events' ) ]
    for k in sorted ( sk  ) :
        if k == other  : continue 
        for s in sk[k] :
            row = s , '%d' % smpls2 [ s ] , '%d' % smpls1 [ s ]
            table.append ( row )
    title = 'Statistics for %2d PIDCalib samples' % len ( ss ) 
    table = T.table ( table , title = title , prefix = '# ' , alignment = 'lrr' )
    logger.info ( '%s:\n%s' % ( title , table ) )
    
    return sk, smpls1, smpls2 

# =============================================================================
## Run PID-calib machinery
def pid_calib ( FUNC , config ) :
    """ Run PID-calib procedure
    """

    particle  = config.particle
    particles = [ particle ]
    
    if   'P'   == particle.upper ( ) : particles = protons + antiprotons
    elif 'P+'  == particle.upper ( ) : particles = protons
    elif 'P-'  == particle.upper ( ) : particles = antiprotons
    elif 'PI'  == particle.upper ( ) : particles = pions_plus + pions_minus
    elif 'PI+' == particle.upper ( ) : particles = pions_plus
    elif 'PI-' == particle.upper ( ) : particles = pions_minus
    elif 'K'   == particle.upper ( ) : particles = kaons_plus + kaons_minus
    elif 'K+'  == particle.upper ( ) : particles = kaons_plus
    elif 'K-'  == particle.upper ( ) : particles = kaons_minus
    elif 'E'   == particle.upper ( ) : particles = e_plus + e_minus
    elif 'E+'  == particle.upper ( ) : particles = e_plus
    elif 'E-'  == particle.upper ( ) : particles = e_minus
    elif 'MU'  == particle.upper ( ) : particles = muons_plus + muons_minus
    elif 'MU+' == particle.upper ( ) : particles = muons_plus
    elif 'MU-' == particle.upper ( ) : particles = muons_minus
    else :
        logger.error ("Unknown PARTICLE ``%s''" % particle )
        
    polarity = config.polarity

    if 'Both' == polarity : polarity = ['MagUp', 'MagDown']
    else                  : polarity = [ polarity ]

    # =========================================================================
    if not config.TestPath and not config.TestFiles : 
        known = set()
        for version in config.versions :
            if version in PARTICLES :
                pp = PARTICLES[version]
                for year in config.years :
                    if year in pp :                    
                        parts = pp [year ]
                        for p in parts : known |= set ( parts [ p ] )
        particles = set(particles) & known
        particles = tuple(particles)
                        

    if config.collisions in ('pA', 'Ap'):

        pass
    
        ## if 2016 != year:
        ##    logger.error ( 'There are no %s samples for % year' %
        ##                   ( config.collisions , year ) )
        ##    return
        
        ## if 'MagUp' in polarity:
        ##     polarity.remove('MagUp')
        ##     logger.warning ( 'Only MagDown samples are available for %s/%s' %
        ##                     ( config.collisions , year ) )
        ## if 'v4r1' != config.version:
        ##     logger.warning ("Only PIDCalibTuples v4r1 samples exist for %s/%s" %
        ##                     ( config.collision , year ) )
        ##     config.version = 'v4r1'

    ## remove some buggy stuff
    to_remove = set()

    ## if 'v4r1' == config.version and 2015 == year:
    ##     to_remove |= set([
    ##         'Sigmac0_P'    ,
    ##         'Sigmac0_Pbar' ,  ## buggy samples: sWeight==1
    ##         'Sigmacpp_P'   ,
    ##         'Sigmacpp_Pbar',  ## ditto
    ##         'LbLcMu_P'     ,
    ##         'LbLcMu_Pbar'  ,  ## ditto
    ##         'LbLcPi_P'     ,
    ##         'LbLcPi_Pbar'  ,  ## ditto
    ##         'DSt3Pi_KP'    ,
    ##         'DSt3Pi_KM'    ,  ## very low eff: buggy?
    ##         'Phi_KP'       ,
    ##         'Phi_KM'       ,  ## ditto
    ##     ])
    ## elif 'v4r1' == config.version and 2016 == year:
    ##     to_remove |= set([
    ##         'LbLcMu_P',
    ##         'LbLcMu_Pbar',  ## ditto
    ##         'Ds_KP',
    ##         'Ds_KM'
    ##     ])  ## ditto

    ## remove samples
    particles = tuple ( set ( particles ) - to_remove )

    if config.Samples:
        particles = tuple ( config.Samples )

    if config.Regex :
        import re
        pattern = re.compile ( config.Regex )
        particles = tuple (  p for p in particles if pattern.match ( p )  )

    years      = config.years 
    collisions = [config.collisions]

    ## processing function 
    fun          = FUNC    ( config.cuts )

    ## required weight 
    check_weight = getattr ( fun , 'check_weight' , False  )
    if check_weight : check_weight = getattr ( fun , 'weight' ) 

    # =========================================================================
    
    table = [ ('', 'Value' ) ]
    
    row = 'Data taking periods' , str ( config.years  )
    table.append ( row )
    
    row = 'Collisions'          , str ( collisions )
    table.append ( row )

    row = 'Magnet polarity'      , str ( polarity )
    table.append ( row )

    row = 'PIDCalib versions'    , str ( config.versions ) 
    table.append ( row )

    row   = 'PROCESSOR'          , str ( type ( fun ).__name__ ) 
    table.append ( row )

    if config.Samples :
        row = 'Samples'          , str ( config.Samples )
        table.append ( row )

    if config.Regex :
        row = 'Regex' , config.Regex
        table.append ( row )

    row   = 'PARTICLES'          , pprint.pformat ( list ( particles ) )
    table.append ( row )
    
    if config.cuts :
        row   =  'Config cuts'  , config.cuts 
        table.append ( row )
                 
    row   =  'WEIGHT' , fun.weight 
    table.append ( row )

    row   =  'ACCEPTED' , fun.accepted 
    table.append ( row )
    
    row   =  'REJECTED' , fun.rejected
    table.append ( row )
    
    for v,a in zip ( fun.variables() , ( 'X-axis' , 'Y-axis' , 'Z-axis')  ) :         
        row = a , v
        table.append ( row )
        
    for v,a in zip ( fun.binnings () , ( 'X-bins' , 'Y-bins' , 'Z-bins')  ) :         
        row = a , str ( list ( v ) ) 
        table.append ( row )
        
    import ostap.logger.table as T
    table = T.table ( table , title = 'PIDCalib configuration', prefix = '# ' , alignment = 'lw' , indent = '' )
    logger.info ( 'PIDCalib configuration:\n%s' % table ) 

    logger.info ( 80 * '*')

    # =========================================================================

    ## processing function 
    fun          = FUNC    ( config.cuts )

    ## required weight 
    check_weight = getattr ( fun , 'check_weight' , False  )
    if check_weight : check_weight = getattr ( fun , 'weight' )


    if not particles :
        logger.error ( "No samples are deduced! Check configuration!" )                       
                           
    # =========================================================================
    ## Load PID samples
    # =========================================================================
    if config.TestPath:
        try:
            from pidcalib.grid import BKRequest, filesFromBK, hasGridProxy
        except ImportError:
            logger.error( "Cannot import from ``pidcalib.grid'': for ``testpath'' one needs to use Bender" )
            return {}
        if not hasGridProxy():
            logger.error("Valid GRID proxy is required!")
            return {}
        path = config.TestPath
        logger.info('Test data to be loaded from %s' % path)
        data = load_from_grid ( path, particles, maxfiles = config.MaxFiles, verbose = config.verbose )

    elif config.TestFiles:
        
        logger.info ( 'Test files to be loaded from %s' % config.TestFiles )
        testfiles = config.TestFiles 
        if os.path.exists ( testfiles ) and os.path.isdir ( testfiles ) :
            testfiles = os.path.join ( testfiles , '*.pidcalib.root' ) 
        data = load_data (
            testfiles                  ,
            particles                  ,
            tag      = 'TESTFILES'     ,
            maxfiles = config.MaxFiles ,
            verbose  = config.verbose  )
    else :
        data = load_samples (
            particles                    ,
            years      = config.years    ,
            collisions = collisions      ,
            polarity   = polarity        ,
            versions   = config.versions ,
            verbose    = config.verbose  ,
            grid       = config.UseGrid  ,  
            maxfiles   = config.MaxFiles ,            
            use_eos    = config.UseEos   )

    if not data : logger.error ( "No data samples are found!" )
    else : 
        for k in sorted ( data ) :
            logger.debug ( "Found %4d files for %s" %  ( len ( data [ k ] . files ) , k ) )
        

    # =========================================================================
    ## Start some pre processing
    # =========================================================================
    if config.Dump :

        files = set()
        ntest = 5 
        for k in data :
            ffiles =  data[k].files
            ns = min ( len ( ffiles ) , ntest  ) 
            for f in sorted ( random.sample ( ffiles , ns ) ) :  ## get at most "ntest" files 
                files.add ( f )

        logger.info ( 'Inspect %s randomly selected input data files...' % len ( files ) ) 
        check_samples ( files )
        
    # 
    ## get the list of PROBNN tunes for the samples 
    # 
    with timing ( 'Check for PROBBNN tunes... (sequential loop)' , logger = logger ) :        
        tunes = set()                
        for key in progress_bar ( data ) :
            chain    = data [ key ] . chain
            branches = chain.branches() 
            for b in branches :
                for s in ( 'probe_Brunel_', 'probe_' ):
                    ns = len ( s ) 
                    if b.startswith ( s ) :
                        i = b.find ( '_ProbNN' , ns )
                        if ns < i : tunes.add ( b [ ns : i ] )
            del chain
                    
    if tunes:
        tunes = list ( tunes )
        tunes.sort ()
        tunes = [ ( str(i),t) for i,t in enumerate ( tunes , start = 1 )   ]
        table = [ ( '#' , 'Tune' ) ] + tunes
        title = 'Available PROBNN tunes'
        table = T.table ( table , title = title , prefix = '# ' )          
        logger.info    ( 'Available %3d PROBNN-tunes\n%s' % ( len ( tunes ) , table ) ) 
    else :
        logger.warning ( 'No PROBNN-tunes are found!' )

    # =========================================================================
    # Collect statistics for variables
    # =========================================================================

    ## important variables: weights 
    known_weights = set ( [ 'probe_sWeight', 'probe_Brunel_sWeight' ] )
    if check_weight : known_weights.add ( check_weight )  
    known_weights = list ( known_weights )
    known_weights.sort()
    known_weights = tuple ( known_weights )
    
    ## variables for statistics 
    stat_vars = ()
    if config.Dump : 
        stat_vars = fun.variables() + ( fun.accepted , fun.rejected , fun.weight )
        if fun.cuts : stat_vars = stat_vars + ( fun.cuts , )

    ## all interesting variable 
    all_variables = tuple ( set ( stat_vars + known_weights ) )

    statistics = {}
    
    if all_variables :
        
        if config.Parallel and not config.UseFrame :

            chunk_size = config.ChunkSize if 1 <= config.ChunkSize else 20
            
            task = StatVar2Task ( all_variables )
            
            from ostap.parallel.parallel import WorkManager 
            wmgr = WorkManager   ( silent = not config.verbose  , progress = True )
            
            jobs = []            
            for key in  data :
                dk   = data [ key ] 
                name = dk.chain_name
                for chunk in chunked ( dk.files , chunk_size ) :
                    row   = key , name , chunk   
                    jobs.append ( row )
                    
            with timing ( 'Parallel statistics for weights and variables... [%d jobs]' % len ( jobs ) , logger = logger ) :
                        
                wmgr.process ( task , jobs )
                
                statistics = task.results()

                for k in statistics:
                    stat = statistics [k]
                    for v in stat : stat[v] = stat[v].values() 
                    
        else :
                        
            with timing ( 'Sequential statistics for weights and variables... [UseFrame=%s]'  % config.UseFrame , logger = logger ) :
                
                for k in progress_bar ( sorted ( data ) ) :
                    
                    chain = data[k].chain
                    
                    ## get statistics from input files 
                    stats = get_statistics ( chain                       ,
                                             all_variables               ,
                                             use_frame = config.UseFrame ,
                                             parallel  = config.Parallel ) 

                    statistics [ k ]  = stats
            
    # ==========================================================================
    ## table of weights
    # ==========================================================================

    bad_keys = set()
    
    from  ostap.logger.colorized import allright, attention 
    table = [ ( 'Sample' , '#files' , '#events' , '      sWeight' , '  mean +/- rms   ' , '   min / max   ' ) ]
    
    
    logger.info ( 'Check weights for data samples...' ) 
    for k in progress_bar ( sorted ( data ) ) :

        if not k in statistics : continue
        
        statvars = statistics [ k ]
        
        for sw in statvars :

            if not sw in known_weights : continue

            cc  = statvars [ sw ]
            
            c1  = "%+7.4f +/- %-7.4f" % ( cc.mean () , cc.rms () ) 
            c2  = "%+7.3f / %-+7.3f"  % ( cc.min  () , cc.max () ) 
            
            if 0 < cc.rms () and cc.min() < cc.max() : pass
            else                                     : c2  = attention ( c2 )
                
            row = k                                ,\
                  '%3d' % len ( data [ k ].files ) ,\
                  '%8d' % cc.nEntries ()           ,\
                  sw , c1 , c2 
            
            table.append ( row ) 

        if check_weight and check_weight in statvars :
            cc = statvars [ check_weight ]
            mn , mx = cc.minmax()
            if mn == mx : 
                logger.error ( "%-35s: %20s is ``trivial'': mean/(min,max)=%s/(%s,%s), skip it!"
                               % ( k , check_weight , cc.mean().toString('%5.3f+-%-5.3f')  , cc.min() , cc.max() ) )
                bad_keys.add ( k ) 
                
                
    table = T.table ( table , title = 'sWeigth for selected samples' , prefix = '# ')
    logger.info ( 'sWeight for selected samples :\n%s' % table )
        
    
    keys  = data.keys()

    # =========================================================================
    
    files    = set()    
    results  = {}

    # =========================================================================
    ## remove bad data samples
    # =========================================================================
    
    data_ = {}
    for k in sorted ( data ) :
        if k in bad_keys : continue
        data_ [ k ]  = data [ k ] 

    if bad_keys :
        lst  = pprint.pformat ( list ( bad_keys ) ) 
        logger.warning ("Remove from processing %d keys due to trivial ``%s'' weight : \n%s" % ( len ( bad_keys ) ,
                                                                                                 check_weight     ,
                                                                                                 lst              ) ) 
    data = data_
    keys = data.keys()

    if config.Dump :

        for var in stat_vars :
            
            header = ( 'Sample' , '#' , 'mean +/- rms' , 'min/max' )
            table  = [ header ]
            for k in sorted ( statistics ) :

                stats = statistics [ k ]
                if not var in stats : continue
                
                cc     = stats[ var ]
                
                row = ( k , '%d' % cc.nEntries() ,
                '%+11.5g +/- %-11.5g' % ( float ( cc.mean() ) , cc.rms() ) , 
                '%+11.5g / %-11.5g'   % ( cc.min() , cc.max() ) )
                
                table.append ( row )
                
            import ostap.logger.table as T
            table = T.table ( table , title = "Statistics for %s" % var , prefix = '# ' , alignment = 'lrcc'  )
            logger.info ( "Statistics for ``%s'' variable\n%s" %  ( var , table ) ) 
            

    ## parallel processing 
    if config.Parallel and not config.UseFrame :

        task       = PidCalibTask  ( fun            )
        chunk_size = config.ChunkSize if 1 <= config.ChunkSize else 20

        from ostap.parallel.parallel import WorkManager 
        wmgr = WorkManager   ( silent = not config.verbose , progress = True )
        jobs = []
        for key in  data :
            dk    = data [ key ] 
            name  = dk.chain_name
            for chunk in chunked ( dk.files , chunk_size ) :
                row   = key , name , chunk , config.UseFrame , False
                jobs.append ( row )
                
        with timing ( 'Parallel processing of PIDCalib data...[%d jobs]' % len ( jobs )  , logger = logger ) : 
            wmgr.process ( task , jobs )            
            results = task.results()
            
    ## sequential processing 
    else :

        with timing ( 'Sequential processing of PIDCalib data...[UseFrame=%s]' % config.UseFrame , logger = logger ) : 
            keys = data.keys()
            for i , k in enumerate ( progress_bar ( data  ) ) :
                
                logger.debug  ( 'Processing %s [%2d/%-2d]' % ( k , i , len ( keys ) ) )
                
                d         = data [ k ]
                
                acc , rej = fun.run ( d.chain , use_frame = config.UseFrame , parallel = False )
                results [ k ] = acc.clone() , rej.clone() 
                
    # =========================================================================
    keys = results.keys()

    tacc     = None
    trej     = None

    processed = set()
    header  = ( 'Sample' , '#accepted [10^3]' , '#rejected [10^3]' , '<glob-eff> [%]'  , '<diff-eff> [%]' , 'min [%]' , 'max [%]' )
    
    report  = [ header ]

    for k in sorted ( results ) : 

        acc , rej = results [ k ]
        
        na = acc.accumulate () / 1000
        nr = rej.accumulate () / 1000

        heff = 100. / (1. + rej / acc )
        eeff = 100. / (1. + nr  / na  )
        hst  = heff.stat()

        row = k , \
              na.toString   ( '%10.2f +/- %-6.2f' ) ,           \
              nr.toString   ( '%10.2f +/- %-6.2f' ) ,           \
              eeff.toString ( '%6.2f +/- %-5.2f'  ) ,           \
              '%6.2f +/- %-5.2f' % ( hst.mean() , hst.rms() ) , \
              '%+6.2f'           %   hst.min()      ,           \
              '%+6.2f'           %   hst.max()
        
        report.append ( row )
        
        if    tacc : tacc += acc
        else       : tacc = acc.clone()

        if    trej : trej += rej
        else       : trej = rej.clone()

        processed.add ( k )
        
        import datetime
        now = datetime.datetime.now()
        with DBASE.open ( config.output ) as db:
            db [  k                                     ] = acc, rej
            heff = 1.0 / ( 1 + rej / acc )

            ke = k + ':efficiency'
            db [ ke ] = heff            ## efficiency histogram
            
            if   isinstance ( heff , ROOT.TH3 ) and 3 == heff.dim () :
                kz = k + ':efficiency(z-slices)'
                db [  kz ] = [ heff.sliceZ(i) for i in range ( 1 , heff.nbinsz() ) ]
            elif isinstance ( heff , ROOT.TH2 ) and 2 == heff.dim () :
                ky = k + ':efficiency(y-slices)' 
                db [  ky ] = [ heff.sliceY(i) for i in range ( 1 , heff.nbinsy() ) ]
                                               
            db [  k + ':data'                           ] = data[k] 
            db [  k + ':conf'                           ] = config
            db [ 'TOTAL_%s'           % config.particle ] = tacc, trej       ## accumulate 
            db [ 'TOTAL_%s:keys'      % config.particle ] = tuple ( keys  ) 
            db [ 'TOTAL_%s:files'     % config.particle ] = data[k].files 
            db [ 'TOTAL_%s:conf'      % config.particle ] = config
            db [ 'TOTAL_%s:created'   % config.particle ] = now
            db [ 'TOTAL_%s:processed' % config.particle ] = processed

            if   config.TestFiles : db [ 'TESTFILES'] = config.TestFiles
            elif config.TestPath  : db [ 'TESTPATH' ] = config.TestPath

                
    if os.path.exists ( config.output ) :
        with DBASE.open ( config.output , 'r') as db:
            try:
                key     = 'TOTAL_%s' % config.particle
                ta , tr = db [ key ]

                na = ta.accumulate () / 1000
                nr = tr.accumulate () / 1000

                heff = 100. / (1. + tr / ta)
                eeff = 100. / (1. + nr / na)
                
                hst = heff.stat()
                del heff
                
                row = key , \
                      na.toString   ( '%10.2f +/- %-6.2f' ) ,           \
                      nr.toString   ( '%10.2f +/- %-6.2f' ) ,           \
                      eeff.toString (  '%6.2f +/- %-5.2f' ) ,           \
                      '%6.2f +/- %-5.2f' % ( hst.mean() , hst.rms() ) , \
                      '%+6.2f'  % hst.min()                 ,           \
                      '%+6.2f'  % hst.max()
                
                report.append ( row )
                
                logger.info('Output DBASE with results: %s' % config.output )
                db.ls ()
            except:
                pass

    import ostap.logger.table as T
    table = T.table ( report , title = 'Performance for %s processed samples' % len ( keys ) , prefix = '# ')
    logger.info ( 'Performance for %s processed %s samples:\n%s' % ( len ( keys ) , particles , table ) )

    if config.verbose and os.path.exists ( config.output ) and not ROOT.gROOT.IsBatch () :
        
        ROOT.gStyle.SetPalette ( 53 )
        
        with DBASE.open ( config.output , 'r') as db:

            for k in sorted ( results ) :
                
                tag1 = k + ':efficiency'
                if tag1 in db :
                    ## try :
                    heff = db.get ( tag1 , None )
                    if  heff and isinstance ( heff , ROOT.TH1 ) and 2 == heff.dim () :
                        title = 'Sample %25s, 2D-efficiency' %  k
                        with wait ( 2 ) , use_canvas ( title ) , useStyle ('Z' ) : heff.draw('colz')
                    elif heff and isinstance ( heff , ROOT.TH1 ) and 1 == heff.dim () :
                        title = 'Sample %25s, 1D-efficiency' %  k
                        with wait ( 2 ) , use_canvas ( title ) :  heff.draw ()
                    ##except :
                    ##    pass
                    
                tag2 = k + ':efficiency(z-slices)'
                if tag2 in db :
                    ## try :
                    zslices = db.get ( tag2 , [] )
                    for i , zs in enumerate ( zslices ) :
                        title = 'Sample %25s, 2D-efficiency, z-slice #%d' % ( k , i + 1 ) 
                        with wait ( 2 ) , use_canvas ( title ) , useStyle ( 'Z' ): zs.draw('colz') 
                    ##except :
                    ##    pass 
                            
                tag3 = k + ':efficiency(y-slices)'
                if tag3 in db :
                    ## try : 
                    yslices = db.get ( tag3 , [] )
                    for i , ys in enumerate ( yslices ) :
                        title = 'Sample %25s, 1D-efficiency, y-slice #%d' % ( k , i+1 )
                        with wait( 2 ) , use_canvas ( title ) : ys.draw() 
                    ## except :
                    ##    pass

    parts = set () 
    for p in particles :
        for k in keys :
            if k.endswith ( p ) : parts.add ( p )
    parts = list ( parts )
    parts.sort() 

    table = [ ('' , 'Value' )]

    row = 'Data taking periods' , str ( config.years )
    table.append ( row )

    row = 'Collisions'          , str ( collisions   )
    table.append ( row )

    row = 'Magnet polarity'      , str ( polarity    )
    table.append ( row )

    row = 'PIDCalib versions'    , str ( config.versions ) 
    table.append ( row )

    row   = 'PROCESSOR'          , str ( type ( fun ).__name__  ) 
    table.append ( row )

    row   =  'CUTS' , fun.cuts
    table.append ( row )

    ## if config.cuts :
    ##    row   =  'Addtional cuts' , config.cuts 
    ##    table.append ( row )
                 
    row   =  'WEIGHT' , fun.weight 
    table.append ( row )

    row   =  'ACCEPTED' , fun.accepted 
    table.append ( row )
    
    row   =  'REJECTED' , fun.rejected
    table.append ( row )

    for v,a in zip ( fun.variables() , ( 'X-axis' , 'Y-axis' , 'Z-axis')  ) :         
        row = a , v
        table.append ( row )
            
    for v,a in zip ( fun.binnings () , ( 'X-bins' , 'Y-bins' , 'Z-bins')  ) :         
        row = a , str ( list ( v ) ) 
        table.append ( row )
        
    if set ( parts ) != set  ( particles ) :
        row = 'Requested %3d particles ' % len ( particles ) , str ( list ( particles ) )
        row = 'Processed %3d particles ' % len ( parts     ) , str ( list ( parts     ) )
        table.append ( row )
    else :
        row = 'Processed %3d particles ' % len ( particles ) , str ( list ( particles ) )
        table.append ( row )
    
    if bad_keys :
        row = 'Rejected  %3d particles ' % len ( bad_keys  ) , str ( list ( bad_keys ) )
        table.append ( row )

    if config.output :
        row = 'Output database'   , config.output
        table.append ( row )
        
    table = T.table ( table , title = 'PIDCalib final report', prefix = '# ' , alignment = 'lw' , indent = '' )
    logger.info ( 'PIDCalib final report:\n%s' % table ) 
        
    return data

# =============================================================================
## The abstarct base class for processing
#  - It has only one   essential method: <code>run</code>
#  - It should return a tuple of two histograms: accepted and rejected 
class PARTICLE ( object )  :
    """ The abstract base class for processing
    - It has only one   essential method: <code>run</code>
    - It should return a tuple of two histograms: accepted and rejected 
    """
    def __init__ ( self , accepted , rejected , weight , cuts = '' , check_weight = True ) :
        
        self.__accepted     = str ( ROOT.TCut ( accepted ) )
        self.__rejected     = str ( ROOT.TCut ( rejected ) )
        self.__weight       = str ( ROOT.TCut ( weight   ) ) 
        self.__cuts         = str ( ROOT.TCut ( cuts     ) )
        self.__check_weight = True if check_weight else False
        

        if self.cuts:  ## redefine accepted/rejected
            self.__accepted = str ( ROOT.TCut ( self.cuts ) & ROOT.TCut ( self.accepted ) ) 
            self.__rejected = str ( ROOT.TCut ( self.cuts ) & ROOT.TCut ( self.rejected ) ) 
            logger.debug ( "CUTS    : %s" % self.cuts )
            
        self.__accepted = str ( self.accepted * ROOT.TCut ( self.weight ) ) 
        self.__rejected = str ( self.rejected * ROOT.TCut ( self.weight ) ) 

        logger.debug ( "ACCEPTED: %s" % self.accepted )
        logger.debug ( "REJECTED: %s" % self.rejected )
        logger.debug ( "WEIGHT  : %s" % self.weight   )


    # =========================================================================
    ## Abstract methdod to get projective variables 
    @abc.abstractmethod
    def variables ( self ) :
        """Abstract method to get projective variables 
        """
        return  () 

    # =========================================================================
    ## Abstract methdod to get binning schemes 
    @abc.abstractmethod
    def binnings ( self ) :
        """Abstract methdod to get binning schemes
        """
        return () 
        
    ## the  function :-)
    def __call__(self, data):
        return self.run ( data )

    # =========================================================================
    ## Fill accepted/rejected histograms 
    #  @param data input data <code>TChain</code>
    #  @return tuple of histograms  with "accepted" and "rejected" distributions
    def run ( self , data , use_frame = True , parallel = False ) :
        """Abstract method: process the data
        - `data` : input data `TChain`
        - return tuple of histograms  with ``accepted'' and ``rejected'' distributions
        """
        import ROOT
        import ostap.core.pyrouts
        from   ostap.core.meta_info import root_info 
        #
        ## we need here ROOT and Ostap machinery!
        #
        self.ha = self.ha.clone()
        self.hr = self.hr.clone()

        if ( 6 , 25 ) <= root_info  and use_frame :
            
            from ostap.frames.frames import DataFrame, frame_project         
            frame = DataFrame ( data ) ## , enable = True ) 
            
            current = frame
            
            if self.cuts : current = current.Filter ( self.cuts ) 
            
            vars     = self.variables()
            
            vars_acc = vars + ( self.accepted , ) 
            vars_rej = vars + ( self.rejected , )
            
            action_acc = frame_project ( current  , self.ha.model() , *vars_acc )
            action_rej = frame_project ( current  , self.hr.model() , *vars_rej ) 
            
            ha = action_acc.GetValue()
            hr = action_rej.GetValue()
            
        elif parallel : 

            na , ha = data.pproject ( self.ha , self.variables () , self.accepted , use_frame = False )
            nr , hr = data.pproject ( self.hr , self.variables () , self.rejected , use_frame = False )

        else : 

            na , ha = data. project ( self.ha , self.variables () , self.accepted , use_frame = False )
            nr , hr = data. project ( self.hr , self.variables () , self.rejected , use_frame = False )


        ## ha . SetName ( "Accepted" )
        ## hr . SetName ( "Rejected" )

        return ha , hr

    @property
    def accepted ( self ) :
        """``accepted'' : configuration of ``accepted'' sample"""
        return self.__accepted
    @property
    def rejected ( self ) :
        """``rejected'' : configuration of ``rejected'' sample"""
        return self.__rejected
    @property
    def cuts     ( self ) :
        """``cuts'' : cuts to be applied """
        return self.__cuts    
    @property
    def weight   ( self ) :
        """``weight'' : actual (s)weight to select signal events"""
        return self.__weight
    @property
    def check_weight ( self ) :
        """``check_weight'' : check the weight before exectution"""
        return self.__check_weight
    
# =============================================================================
## the actual function to fill PIDcalib histograms
#  - it books two histograms  (1D in this case)
#  - it fill them with 'accepted' and 'rejected' events (1D in this case)
#  @author Vanya BELYAEV Ivan.Belyaev@itep.ru
#  @date   2014-05-10
class PARTICLE_1D(PARTICLE):
    """The actual function to fill PIDcalib histograms
    - it books two histograms  (1D in this case)  ``accepted'' and ``rejected''
    - it fill them with 'accepted' and 'rejected' events (1D in this case)
    - finally one calculates the efficiency as
    >>> efficiency = 1/( 1 + rejected/accepted)
    """

    ## create the object
    def __init__ ( self                ,
                   accepted            , ## accepted sample
                   rejected            , ## rejected sample
                   xbins               , ## bins in 1st axis                  
                   xvar                , ## e.g.'probe_Brunel_P/1000' 
                   weight              , ## weight, e.g. 'probe_sWeight'
                   cuts         = ''   , ## additional cuts (if any)
                   check_weight = True ) :
        #
        ## the heart of the whole game:   DEFINE PID CUTS!
        #
        PARTICLE.__init__ ( self ,
                            accepted     = accepted     ,
                            rejected     = rejected     ,
                            weight       = weight       ,
                            cuts         = cuts         ,
                            check_weight = check_weight )
        #
        ## book 1D-histograms
        #
        import ROOT
        import ostap.core.pyrouts  
        from   ostap.histos.histos import h1_axis
        
        self.__xvar = str ( ROOT.TCut ( xvar ) ) 
        
        self.ha = h1_axis ( xbins , title='Accepted(%s)' % self.accepted )
        self.hr = h1_axis ( xbins , title='Rejected(%s)' % self.rejected ) 

        self.__xbins = tuple ( xbins )
        
    @property
    def xvar ( self ) :
        """``xvar'' : x-variable for efficiency histograms"""
        return self.__xvar 
    @property
    def xbins ( self ) :
        """``xbins'' : binning for x-variable for efficiency histograms"""
        return self.__xbins
        
    ## 
    def variables ( self )  :
        return self.xvar ,
    def binnings  ( self ) :
        return self.xbins, 
    
# =============================================================================
## the actual function to fill PIDcalib histograms
#  - it books two histograms  (2D in this case)
#  - it fill them with 'accepted' and 'rejected' events (2D in this case)
#  @author Vanya BELYAEV Ivan.Belyaev@itep.ru
#  @date   2014-05-10
class PARTICLE_2D(PARTICLE_1D):
    """The actual function to fill PIDcalib histograms
    - it books two histograms  (2D in this case)  ``accepted'' and ``rejected''
    - it fill them with 'accepted' and 'rejected' events (2D in this case)
    - finally one calculates the efficiency as
    >>> efficiency = 1/( 1 + rejected/accepted)
    """

    ## create the object
    def __init__ ( self                ,
                   accepted            , ## accepted sample
                   rejected            , ## rejected sample
                   xbins               , ## bins in 1st axis
                   ybins               , ## bins in 2nd axis
                   xvar                , ## e.g.'probe_Brunel_P/1000' 
                   yvar                , ## e.g.'probe_Brunel_ETA' 
                   weight              , ## weight, e.g. 'probe_sWeight'
                   cuts         = ''   , ## additional cuts (if any)
                   check_weight = True ) :
        #
        ## initialize the base class
        #
        PARTICLE_1D.__init__ ( self                        ,
                               accepted     = accepted     ,
                               rejected     = rejected     ,
                               xbins        = xbins        ,
                               xvar         = xvar         ,
                               weight       = weight       , 
                               cuts         = cuts         ,
                               check_weight = check_weight )
        #
        ## book 2D-histograms
        #
        import ROOT
        import ostap.core.pyrouts
        from   ostap.histos.histos import h2_axes
        
        self.__yvar = str ( ROOT.TCut ( yvar ) ) 

        self.ha = h2_axes ( xbins , ybins , title = 'Accepted(%s)' % self.accepted )
        self.hr = h2_axes ( xbins , ybins , title = 'Rejected(%s)' % self.rejected )

        ## assert 2 == len ( self.variables () ) , 'PARTCILE_2D: Invalid setting!'
       
        self.__ybins = tuple ( ybins )
 
    @property
    def yvar ( self ) :
        """``yvar'' : y-variable for efficiency histograms"""
        return self.__yvar 
    @property
    def ybins ( self ) :
        """``ybins'' : binning for y-variable for efficiency histograms"""
        return self.__ybins
 
    ## 
    def variables ( self )  :
        return self.xvar  , self.yvar 
    def binnings  ( self ) :
        return self.xbins , self.ybins 
 
# =============================================================================
## the actual function to fill PIDcalib histograms
#  - it books two histograms  (3D in this case)
#  - it fill them with 'accepted' and 'rejected' events (3D in this case)
#  @author Vanya BELYAEV Ivan.Belyaev@itep.ru
#  @date   2014-05-10
class PARTICLE_3D(PARTICLE_2D):
    """The actual function to fill PIDcalib histograms
    - it books two histograms  (3D in this case)  ``accepted'' and ``rejected''
    - it fill them with 'accepted' and 'rejected' events (3D in this case)
    - finally one calculates the efficiency as
    >>> efficiency = 1/( 1 + rejected/accepted)
    """

    ## create the object
    def __init__ ( self                ,
                   accepted            , ## accepted sample
                   rejected            , ## rejected sample
                   xbins               , ## bins in 1st axis
                   ybins               , ## bins in 2nd axis
                   zbins               , ## bins in 3rd axis
                   xvar                , ## e.g.'probe_Brunel_P/1000' 
                   yvar                , ## e.g.'probe_Brunel_ETA' 
                   zvar                , ## e.g.'nTracks' 
                   weight              , ## weight, e.g. 'probe_sWeight'
                   cuts         = ''   , ## additional cuts (if any)
                   check_weight = True ) :

        ## initialize the base class
        PARTICLE_2D.__init__ ( self         ,
                               accepted     = accepted     ,
                               rejected     = rejected     ,
                               xbins        = xbins        ,
                               ybins        = ybins        ,
                               xvar         = xvar         ,
                               yvar         = yvar         ,
                               weight       = weight       , 
                               cuts         = cuts         ,
                               check_weight = check_weight )
        
        import ROOT
        import ostap.core.pyrouts
        from   ostap.histos.histos import h3_axes

        self.__zvar = str ( ROOT.TCut ( zvar ) ) 

        self.ha = h3_axes ( xbins , ybins , zbins , title = 'Accepted(%s)' % accepted )
        self.hr = h3_axes ( xbins , ybins , zbins , title = 'Rejected(%s)' % rejected )

        ## assert 3 == len ( self.variables () ) , 'PARTCILE_3D: Invalid setting!'
        
        self.__zbins = tuple ( zbins )
        
    @property
    def zvar ( self ) :
        """``zvar'' : z-variable for efficiency histograms"""
        return self.__zvar 
    @property
    def zbins ( self ) :
        """``zbins'' : binning for z-variable for efficiency histograms"""
        return self.__zbins

    ## 
    def variables ( self )  :
        return self.xvar  , self.yvar  , self.zvar 
    def binnings  ( self ) :
        return self.xbins , self.ybins , self.zbins 
    
# =============================================================================
if '__main__' == __name__ :
    
    from ostap.utils.docme import docme
    docme ( __name__ , logger = logger )

    run_pid_calib ( None , [ '-h'] )
    
# =============================================================================
##                                                                      The END
# =============================================================================
