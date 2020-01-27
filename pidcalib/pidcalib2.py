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
""" Module to run LHCb/PIDCalib machinery for RUN-II data
- see https://twiki.cern.ch/twiki/bin/view/LHCbPhysics/ChargedPID
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
from __future__ import absolute_import
import ROOT, os, abc 
# =============================================================================
from  ostap.logger.logger import getLogger, setLogging
if '__main__' == __name__: logger = getLogger ( 'ostap.pidcalib2' )
else                     : logger = getLogger ( __name__          )
# =============================================================================
import ostap.core.pyrouts
import ostap.trees.trees
import ostap.trees.cuts 
import ostap.io.zipshelve as DBASE
# =============================================================================
# PIDCALIB data samples (LHCb Bookeeping DB-paths)
# =============================================================================
bookkeeping_paths = {
    ## 2016, v4r1 
    'pp/2016/v4r1/MagUp'   : '/LHCb/Collision16/Beam6500GeV-VeloClosed-MagUp/Real Data/Reco16/Turbo02a/PIDCalibTuples4r1/PIDMerge01/95100000/PIDCALIB.ROOT'   , 
    'pp/2016/v4r1/MagDown' : '/LHCb/Collision16/Beam6500GeV-VeloClosed-MagDown/Real Data/Reco16/Turbo02a/PIDCalibTuples4r1/PIDMerge01/95100000/PIDCALIB.ROOT' ,
    ## 2016, v5r1 
    'pp/2016/v5r1/MagUp'   : '/LHCb/Collision16/Beam6500GeV-VeloClosed-MagUp/Real Data/Reco16/Turbo02a/PIDCalibTuples5r1/PIDMerge05/95100000/PIDCALIB.ROOT'   , 
    'pp/2016/v5r1/MagDown' : '/LHCb/Collision16/Beam6500GeV-VeloClosed-MagDown/Real Data/Reco16/Turbo02a/PIDCalibTuples5r1/PIDMerge05/95100000/PIDCALIB.ROOT' ,
    ## 2015, v4r1 
    'pp/2015/v4r1/MagUp'   : '/LHCb/Collision15/Beam6500GeV-VeloClosed-MagUp/Real Data/Reco15a/Turbo02/PIDCalibTuples4r1/PIDMerge01/95100000/PIDCALIB.ROOT'   , 
    'pp/2015/v4r1/MagDown' : '/LHCb/Collision15/Beam6500GeV-VeloClosed-MagDown/Real Data/Reco15a/Turbo02/PIDCalibTuples4r1/PIDMerge01/95100000/PIDCALIB.ROOT' ,
    ## 2015, v5r1 
    'pp/2015/v5r1/MagUp'   : '/LHCb/Collision15/Beam6500GeV-VeloClosed-MagUp/Real Data/Reco15a/Turbo02/PIDCalibTuples5r1/PIDMerge05/95100000/PIDCALIB.ROOT'   , 
    'pp/2015/v5r1/MagDown' : '/LHCb/Collision15/Beam6500GeV-VeloClosed-MagDown/Real Data/Reco15a/Turbo02/PIDCalibTuples5r1/PIDMerge05/95100000/PIDCALIB.ROOT' ,
    ##
    'Ap/2016/v5r0/MagDown' : '/LHCb/Ionproton16/Beam6500GeV-VeloClosed-MagDown/Real Data/Reco16pLead/Turbo03pLead/PIDCalibTuples5r0/PIDMerge01/95100000/PIDCALIB.ROOT' ,
    'pA/2016/v5r0/MagDown' : '/LHCb/Protonion16/Beam6500GeV-VeloClosed-MagDown/Real Data/Reco16pLead/Turbo03pLead/PIDCalibTuples5r0/PIDMerge01/95100000/PIDCALIB.ROOT' ,
    ##2017
    'Ap/2017/v8r1/MagDown' : '/LHCb/Ionproton16/Beam6500GeV-VeloClosed-MagDown/Real Data/Reco16pLead/Turbo03pLead/PIDCalibTuples5r0/PIDMerge01/95100000/PIDCALIB.ROOT' ,
    'pA/2017/v8r1/MagDown' : '/LHCb/Protonion16/Beam6500GeV-VeloClosed-MagDown/Real Data/Reco16pLead/Turbo03pLead/PIDCalibTuples5r0/PIDMerge01/95100000/PIDCALIB.ROOT' ,
    #2017
    'pp/2017/v8r1/MagUp'   : '/LHCb/Collision17/Beam6500GeV-VeloClosed-MagUp/Real Data/Reco17/Turbo04/PIDCalibTuples8r1/PIDMerge05/95100000/PIDCALIB.ROOT' ,
    'pp/2017/v8r1/MagDown' : '/LHCb/Collision17/Beam6500GeV-VeloClosed-MagDown/Real Data/Reco17/Turbo04/PIDCalibTuples8r1/PIDMerge05/95100000/PIDCALIB.ROOT' ,
    #2018
    'pp/2018/v8r0/MagUp'   : '/LHCb/Collision18/Beam6500GeV-VeloClosed-MagUp/Real Data/Reco18/Turbo05/PIDCalibTuples8r0/PIDMerge05/95100000/PIDCALIB.ROOT' ,
    'pp/2018/v8r0/MagDown' : '/LHCb/Collision18/Beam6500GeV-VeloClosed-MagDown/Real Data/Reco18/Turbo05/PIDCalibTuples8r0/PIDMerge05/95100000/PIDCALIB.ROOT' ,
    }
# =============================================================================
## PIDCALIB data samples
#  https://twiki.cern.ch/twiki/bin/view/LHCbPhysics/ChargedPID
samples  = {
    ## PIDCalibTuples v4r1     
    'v4r1' : {
    'pp/2015/MagUp'   : '/eos/lhcb/grid/prod/lhcb/LHCb/Collision15/PIDCALIB.ROOT/00057800/0000/' ,
    'pp/2015/MagDown' : '/eos/lhcb/grid/prod/lhcb/LHCb/Collision15/PIDCALIB.ROOT/00057802/0000/' ,
    'pp/2016/MagUp'   : '/eos/lhcb/grid/prod/lhcb/LHCb/Collision16/PIDCALIB.ROOT/00056408/0000/' ,
    'pp/2016/MagDown' : '/eos/lhcb/grid/prod/lhcb/LHCb/Collision16/PIDCALIB.ROOT/00056409/0000/' ,
    'pA/2016/MagDown' : '/eos/lhcb/grid/prod/lhcb/LHCb/Protonion16/PIDCALIB.ROOT/00058286/0000/' ,
    'Ap/2016/MagDown' : '/eos/lhcb/grid/prod/lhcb/LHCb/Ionproton16/PIDCALIB.ROOT/00058288/0000/' ,
    },
    ## PIDCalibTuples v5r1 
    'v5r1' : {
    'pp/2015/MagUp'   : '/eos/lhcb/grid/prod/lhcb/LHCb/Collision15/PIDCALIB.ROOT/00064787/0000/' ,
    'pp/2015/MagDown' : '/eos/lhcb/grid/prod/lhcb/LHCb/Collision15/PIDCALIB.ROOT/00064785/0000/' ,
    'pp/2016/MagUp'   : '/eos/lhcb/grid/prod/lhcb/LHCb/Collision16/PIDCALIB.ROOT/00064793/0000/' ,
    'pp/2016/MagDown' : '/eos/lhcb/grid/prod/lhcb/LHCb/Collision16/PIDCALIB.ROOT/00064795/0000/' ,
    },
    ## PIDCalibTuples v8r1 
    'v8r1' : {
    'pp/2017/MagDown' : '/eos/lhcb/grid/prod/lhcb/LHCb/Collision17/PIDCALIB.ROOT/00090823/0000/',
    'pp/2017/MagUp'   : '/eos/lhcb/grid/prod/lhcb/LHCb/Collision17/PIDCALIB.ROOT/00090825/0000/',
    },
    ## PIDCalibTuples v8r0 
    'v8r0' : {
    'pp/2018/MagUp'   : '/eos/lhcb/grid/prod/lhcb/LHCb/Collision18/PIDCALIB.ROOT/00082947/0000/',
    'pp/2018/MagDown' : '/eos/lhcb/grid/prod/lhcb/LHCb/Collision18/PIDCALIB.ROOT/00082949/0000/',
    }
    }
# =============================================================================
## knowno species of particles 
SPECIES   = ( 'EP'  , 'EM'   ,
              'KP'  , 'KM'   ,
              'MuP' , 'MuM'  , 
              'PiP' , 'PiM'  ,
              'P'   , 'Pbar' )
# =============================================================================
## calibration samples 
PARTICLES = {
    'v4r1' : {
    2015 : {
    'ELECTRONS' : ['B_Jpsi_EP'  , 'Jpsi_EP'] ,
    'KAONS'     : ['DSt3Pi_KP'  , 'DSt_KP'   , 'DsPhi_KP'   , 'Phi_KP'  ] ,
    'MUONS'     : ['B_Jpsi_MuP' , 'DsPhi_MuP', 'Jpsi_MuP'   , 'Phi_MuP' ] ,
    'PIONS'     : ['DSt3Pi_PiP' , 'DSt_PiP'  , 'KS_PiP'     ]             ,
    'PROTONS'   : ['Lam0_HPT_P' , 'Lam0_P'   ,                           ## 'B_Jpsi_P'   , 'Jpsi_P' 
                   'Lam0_VHPT_P', 'LbLcMu_P' , 'LbLcPi_P'   , 'Sigmac0_P' , 'Sigmacpp_P' ]
    },
    2016 : {
    'KAONS'     : ['DSt_KP'     , 'DsPhi_KP'  , 'Ds_KP'    ]  ,
    'MUONS'     : ['B_Jpsi_MuP' , 'DsPhi_MuP' , 'Jpsi_MuP' ]  ,
    'PIONS'     : ['DSt_PiP'    , 'KS_PiP'    ]               ,
    'PROTONS'   : ['Lam0_HPT_P' , 'Lam0_P'    , 'Lam0_VHPT_P' , 'LbLcMu_P' ] 
    }} ,
    'v5r1' : {
    2015: {
    'ELECTRONS' : ['B_Jpsi_EP']  ,
    'KAONS'     : ['DSt_KP'      , 'DsPhi_KP'  ]                  ,
    'MUONS'     : ['B_Jpsi_MuP'  , 'Jpsi_MuP'  ] ,
    'PIONS'     : ['DSt_PiP'     , 'KSLL_PiP'  ]                  ,
    'PROTONS'   : ['Lam0LL_HPT_P', 'Lam0LL_P'  , 'Lam0LL_VHPT_P'  , 'LbLcMu_P' ]
    } ,
    2016 : {
    'ELECTRONS' : ['B_Jpsi_EP'   ]   ,
    'KAONS'     : ['DSt_KP'      , 'DsPhi_KP' ] ,
    'MUONS'     : ['B_Jpsi_MuP'  , 'DsPhi_MuP', 'Jpsi_MuP'      ] ,
    'PIONS'     : ['DSt_PiP'     , 'KSLL_PiP' ] ,
    'PROTONS'   : ['Lam0LL_HPT_P', 'Lam0LL_P' , 'Lam0LL_VHPT_P' , 'LbLcMu_P'     ] 
    }
    },
     'v8r1':{
    2017 : {
    'ELECTRONS' : ['B_Jpsi_EP'   ]   ,
    'KAONS'     : ['DSt_KP'      , 'DsPhi_KP' ] ,
    'MUONS'     : ['B_Jpsi_MuP'  , 'DsPhi_MuP', 'Jpsi_MuP'      ] ,
    'PIONS'     : ['DSt_PiP'     , 'KSLL_PiP' ] ,
    'PROTONS'   : ['Lam0LL_HPT_P', 'Lam0LL_P' , 'Lam0LL_VHPT_P' , 'LbLcMu_P' , 'Lc_P' ] 
    }
    },
     'v8r0':{
    2018 : {
    'ELECTRONS' : [ 'B_Jpsi_EP'   ]   ,
    'KAONS'     : [ 'DSt_KP'      , 'DsPhi_KP' ] ,
    'MUONS'     : [ 'B_Jpsi_MuP'  , 'DsPhi_MuP', 'Jpsi_MuP'      ] ,
    'PIONS'     : [ 'DSt_PiP'     , 'KSLL_PiP' ] ,
    'PROTONS'   : [ 'Lam0LL_HPT_P', 'Lam0LL_P' , 'Lam0LL_VHPT_P' , 'LbLcMu_P' , 'Lc_P' ] 
    }
    }
    }
# =============================================================================
# 2015 v4r1
# Ostap.PidCalib2           INFO    Found sample ELECTRONS : ['B_Jpsi_EP', 'Jpsi_EP']
# Ostap.PidCalib2           INFO    Found sample     KAONS : ['DSt3Pi_KP', 'DSt_KP', 'DsPhi_KP', 'Phi_KP']
# Ostap.PidCalib2           INFO    Found sample     MUONS : ['B_Jpsi_MuP', 'DsPhi_MuP', 'Jpsi_MuP', 'Phi_MuP']
# Ostap.PidCalib2           INFO    Found sample     OTHER : ['DsPhi_KM_notag', 'DsPhi_KP_notag', 'Lam0_P_isMuon', 'Lam0_Pbar_isMuon', 'Phi_KM_notag', 'Phi_KP_notag']
# Ostap.PidCalib2           INFO    Found sample     PIONS : ['DSt3Pi_PiP', 'DSt_PiP', 'KS_PiP']
# Ostap.PidCalib2           INFO    Found sample   PROTONS : ['B_Jpsi_P', 'Jpsi_P', 'Lam0_HPT_P', 'Lam0_P', 'Lam0_VHPT_P', 'LbLcMu_P', 'LbLcPi_P', 'Sigmac0_P', 'Sigmacpp_P']

# 2016 v4r1
# Ostap.PidCalib2           INFO    Found sample     KAONS : ['DSt_KP', 'DsPhi_KP', 'Ds_KP']
# Ostap.PidCalib2           INFO    Found sample     MUONS : ['B_Jpsi_MuP', 'DsPhi_MuP', 'Jpsi_MuP']
# Ostap.PidCalib2           INFO    Found sample     OTHER : ['Lam0_P_isMuon', 'Lam0_Pbar_isMuon']
# Ostap.PidCalib2           INFO    Found sample     PIONS : ['DSt_PiP', 'KS_PiP']
# Ostap.PidCalib2           INFO    Found sample   PROTONS : ['Lam0_HPT_P', 'Lam0_P', 'Lam0_VHPT_P', 'LbLcMu_P']

# 2015 v5r1
# Ostap.PidCalib2           INFO    Found sample ELECTRONS : ['B_Jpsi_EP']
# Ostap.PidCalib2           INFO    Found sample     KAONS : ['DSt_KP', 'DsPhi_KP']
# Ostap.PidCalib2           INFO    Found sample     MUONS : ['B_Jpsi_MuP', 'Jpsi_MuP', 'Jpsinopt_MuP']
# Ostap.PidCalib2           INFO    Found sample     OTHER : ['Lam0LL_P_isMuon', 'Lam0LL_Pbar_isMuon']
# Ostap.PidCalib2           INFO    Found sample     PIONS : ['DSt_PiP', 'KSLL_PiP']
# Ostap.PidCalib2           INFO    Found sample   PROTONS : ['Lam0LL_HPT_P', 'Lam0LL_P', 'Lam0LL_VHPT_P', 'LbLcMu_P']

# 2016 v5r1
# Ostap.PidCalib2           INFO    Found sample ELECTRONS : ['B_Jpsi_EP']
# Ostap.PidCalib2           INFO    Found sample     KAONS : ['DSt_KP', 'DsPhi_KP']
# Ostap.PidCalib2           INFO    Found sample     MUONS : ['B_Jpsi_MuP', 'DsPhi_MuP', 'Jpsi_MuP', 'Jpsinopt_MuP']
# Ostap.PidCalib2           INFO    Found sample     OTHER : ['Lam0LL_P_isMuon', 'Lam0LL_Pbar_isMuon']
# Ostap.PidCalib2           INFO    Found sample     PIONS : ['DSt_PiP', 'KSLL_PiP']
# Ostap.PidCalib2           INFO    Found sample   PROTONS : ['Lam0LL_HPT_P', 'Lam0LL_P', 'Lam0LL_VHPT_P', 'LbLcMu_P']
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
                ps |= set ( p.replace ( '_P' , '_Pbar' ) for p in ps )
            PARTICLES [ v ] [ y ] [ k ] = tuple ( ps )


# =============================================================================
## prepare the parser
def make_parser():
    """ Prepare the parser
    - oversimplified version of parser from MakePerfHistsRunRange.py script
    """
    import argparse, os, sys

    parser = argparse.ArgumentParser(
        formatter_class = argparse.RawDescriptionHelpFormatter,
        prog            = os.path.basename ( sys.argv[0] ) ,
        description     = """Make performance histograms for a given:
        a) data taking period <YEAR>        ( e.g. 2015    )
        b) magnet polarity    <MAGNET>      ( 'MagUp' or 'MagDown' or 'Both' )
        c) particle type      <PARTICLE>    ( 'K', 'P' , 'Pi' , 'e' , 'Mu'   )
        """,
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
        '--year',
        metavar = '<YEAR>',
        type    = int,
        choices = ( 2015 , 2016 , 2017 , 2018 ),
        help    = "Data taking period")

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
        help          = "The maximum number of calibration files to process")

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
        '--output'                 ,
        type       = str           ,
        default    = 'PIDCALIB.db' ,
        help       = "The name of output database file")

    parser.add_argument(
        '-v'                  ,
        '--version'           ,
        default = 'v5r1'      ,
        metavar = '<VERSION>' ,
        type    = str         ,
        choices = ( 'v4r1' ,  'v5r0' , 'v5r1' , 'v8r0' , 'v8r1' ) ,
        help    = "Version of PIDCalibTuples to be used")
    parser.add_argument(
        '-s'                       ,
        '--samples'                ,
        default      = []          ,
        dest         = 'Samples'   ,
        metavar      = '<SAMPLES>' ,
        nargs        = '*'         ,
        help         = 'The (test) samples to be processed')
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
        help         = 'The path in DB (year, polarity, etc are ignored)' )

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
        help    = "Use local eos, where possible")
    addGroup.add_argument(
        "-z"                   ,
        "--parallel"           ,
        dest    = "Parallel"   ,
        action  = "store_true" ,
        default = False        ,
        help    = "Use parallelization" )
    addGroup.add_argument(
        "-d"                   ,
        "--dump"               ,
        dest    = "Dump"       ,
        action  = "store_true" ,
        default = False        ,
        help    = "Dump useful statistics and exit" )

    return parser


# =============================================================================
## load certain calibration files  using given file patterns
def load_data ( pattern , particles , tag= '' , maxfiles = -1, verbose = False, data={}):
    """Load certain calibration files  using given file patterns
    """

    from ostap.trees.data import Data

    ikey = 0 
    for p in particles:

        ikey  += 1 
        chain  = p + 'Tuple/DecayTree'
        d      = Data ( chain , pattern , maxfiles = maxfiles , silent = not verbose )
        key    = '%s/%s' % ( tag , p )

        logger.info ( 'Loaded data [%2d/%-2d] for key %s: %s' % ( ikey , len  ( particles ) , key , d ) )
        if not d:
            logger.warning ( 'No useful data is found for %s' % key )
            continue

        data[key] = d

    return data


# =============================================================================
## Load calibration samples
def load_samples ( particles,
                   years      = ( '2015', '2016','2017','2018') ,
                   collisions = ( 'pp'  , 'pA'  , 'Ap' )        , 
                   polarity   = ( 'MagDown' , 'MagUp'  )        ,
                   version    ='v5r1' ,
                   maxfiles   = -1    ,
                   verbose    = False ,
                   use_eos    = False ):
    """Load calibration samples
    """

    try:
        from .grid import hasGridProxy
        if hasGridProxy():
            return load_samples_from_grid ( particles  = particles  ,
                                            years      = years      ,
                                            collisions = collisions ,
                                            polarity   = polarity   ,
                                            version    = version    ,
                                            maxfiles   = maxfiles   ,
                                            verbose    = verbose    ,
                                            use_eos    = use_eos    )
        logger.warning("No grid proxy, switch off to local look-up")
    except ImportError:
        logger.warning("Can't import ``grid'', switch off to local look-up")

    if 0 < maxfiles:
        logger.warning('Only max=%d files will be processed!' % maxfiles)

    maxfiles = maxfiles if 0 < maxfiles else 1000000
    data = {}
    for y in years:
        for c in collisions:
            for p in polarity:
                tag = '%s/%s/%s' % (c, y, p)
                fdir = samples[version].get(tag, None)
                if not fdir:
                    logger.warning(
                        'No data is found for Collisions="%s" , Year="%s" , Polarity="%s"'
                        % (c, y, p))
                    continue

                ## file pattern:
                pattern = fdir + '*.pidcalib.root'

                ## load files
                data = load_data ( pattern   ,
                                   particles ,
                                   tag       ,
                                   maxfiles  ,
                                   verbose   ,
                                   data      )

    return data


# =============================================================================
## Load calibration samples
def load_samples_from_grid ( particles,
                             years      = ('2015', '2016','2017','2028'),
                             collisions = ('pp'   , 'pA'   , 'Ap'),
                             polarity   = ('MagDown', 'MagUp') ,
                             version    ='v5r1' ,
                             maxfiles   = -1    ,
                             verbose    = False ,
                             use_eos    = False ) :
    """Load calibration samples from GRID
    """

    the_path = '/LHCb/{collision}{year}/Beam6500GeV-VeloClosed-{magnet}/Real Data/Reco{reco}/Turbo{turbo}/PIDCalibTuples{version}/PIDMerge{merge}/95100000/PIDCALIB.ROOT'

    maxfiles = maxfiles if 0 < maxfiles else 1000000
    data = {}
    for year in years:
        for c in collisions:
            for magnet in polarity:

                key = '%s/%s/%s/%s' % ( c, year, version, magnet)

                path = bookkeeping_paths.get ( key , '' )
                if not path:
                    logger.warning("Can't find bookeeping entry for %s" % key)
                    continue

                new_data = load_from_grid (
                    path                ,
                    particles           ,
                    tag      = key      ,
                    maxfiles = maxfiles ,
                    verbose  = verbose  ,
                    use_eos  = use_eos  )
                data.update ( new_data )

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
        from grid import BKRequest, filesFromBK
    except ImportError:
        logger.error("Can't import from ``grid''")
        return {}

    logger.debug('Make a try with path "%s"' % path)

    data = {}

    request  = BKRequest(
        path = path, nmax = maxfiles, accessURL = True)  ## , SEs = 'CERN-DST-EOS' )
    files    = filesFromBK ( request )

    # =========================================================================
    if use_eos : 
        files_ = []
        eos_   = 'root://eoslhcb.cern.ch/'
        for f in files :
            if f.startswith ( eos_ ) :
                ff = f[len(eos_):]
                if os.path.exists ( ff ) and os.path.isfile ( ff ) and os.access ( ff , os.R_OK ) :
                    files_.append ( ff )
                    continue
                files_.append ( f )        
        files = files_
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
        files = [
            'root://eoslhcb.cern.ch//eos/lhcb/grid/prod' + f for f in files
        ]

    ## load files
    if files:
        logger.info ( 'Got %d files from "%s"' % ( len ( files ) , path ) ) 
        if verbose and dirs: logger.info('EOS-directories: %s' % list ( dirs ) )
        if not tag: tag = path
        data = load_data ( files , particles , tag , maxfiles , verbose , data )

    if data and dirs:
        logger.info('EOS directories: %s ' % list ( dirs ) )

    return data


# =============================================================================
## Run PID-calib machinery
def run_pid_calib(FUNC, args=[]):
    """ Run PID-calib procedure
    """

    import sys
    vargs = args + [a for a in sys.argv[1:] if '--' != a]

    parser = make_parser()
    config = parser.parse_args(vargs)

    if config.TestPath:
        logger.warning(
            'TestPath:  Year/Polarity/Collision/Version will be ignored')
    elif config.TestFiles:
        logger.warning(
            'TestFiles: Year/Polarity/Collision/Version will be ignored')

    if config.verbose:
        ## import Ostap.Line
        logger.info ( __file__ )
        logger.info ( 80 * '*' )
        logger.info ( __doc__  )
        logger.info ( 80 * '*' )
        _vars = vars ( config )
        _keys = _vars.keys()
        _keys.sort()
        logger.info ( 'PIDCalib configuration:' )
        for _k in _keys:
            logger.info ('  %15s : %-s ' % ( _k , _vars [ _k ] ) )
        logger.info ( 80 * '*')
        setLogging(2)

    if config.verbose: from ostap.logger.logger import logInfo    as useLog
    else:              from ostap.logger.logger import logWarning as useLog

    ## with useLog():
    return pid_calib(FUNC, config)

def treat_arguments(config):
    pass


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
        ## unpack the 
        key , name , files  = item 

        import ROOT
        import ostap.core.pyrouts
        
        data = ROOT.TChain  ( name )
        for f in files : data.Add ( f )
        
        self.__output = {  key : self.__pidfunc.run ( data ) } 
        return self.__output 

    ## get the results 
    def results (  self ) : return self.__output 

    ## merge the results 
    def merge_results  ( self, results ) :

        while results :
            key , item  = results.popitem()
            a   , r     = item
            if key in self.__output :
                sa , sr = self.__output[k]
                sa.Add ( a ) 
                sr.Add ( r ) 
                self.__output[k] = sa , sr
                del a
                del r 
            else  :
                self.__output[k] =  a ,  r 

        
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

    polarity = config.polarity

    if 'Both' == polarity: polarity = ['MagUp', 'MagDown']
    else: polarity = [polarity]

    year = config.year

    try:
        known = set()
        pl = PARTICLES[config.version][year]
        for p in pl:
            known |= set(pl[p])
    except:
        pass

    particles = set(particles) & known
    particles = tuple(particles)

    if config.collisions in ('pA', 'Ap'):
        
        if 2016 != year:
            logger.error ( 'There are no %s samples for % year' %
                           ( config.collisions , year ) )
            return
        if 'MagUp' in polarity:
            polarity.remove('MagUp')
            logger.warning ( 'Only MagDown samples are available for %s/%s' %
                            ( config.collisions , year ) )
        if 'v4r1' != config.version:
            logger.warning ("Only PIDCalibTuples v4r1 samples exist for %s/%s" %
                            ( config.collision , year ) )
            config.version = 'v4r1'

    ## remove some buggy stuff
    to_remove = set()

    if 'v4r1' == config.version and 2015 == year:
        to_remove |= set([
            'Sigmac0_P'    ,
            'Sigmac0_Pbar' ,  ## buggy samples: sWeight==1
            'Sigmacpp_P'   ,
            'Sigmacpp_Pbar',  ## ditto
            'LbLcMu_P'     ,
            'LbLcMu_Pbar'  ,  ## ditto
            'LbLcPi_P'     ,
            'LbLcPi_Pbar'  ,  ## ditto
            'DSt3Pi_KP'    ,
            'DSt3Pi_KM'    ,  ## very low eff: buggy?
            'Phi_KP'       ,
            'Phi_KM'       ,  ## ditto
        ])
    elif 'v4r1' == config.version and 2016 == year:
        to_remove |= set([
            'LbLcMu_P',
            'LbLcMu_Pbar',  ## ditto
            'Ds_KP',
            'Ds_KM'
        ])  ## ditto

    ## remove samples
    particles = tuple ( set ( particles ) - to_remove )

    if config.Samples:
        particles = tuple ( config.Samples )

    year = [year]
    collisions = [config.collisions]
    logger.info ( 'Data taking periods : %s' % year               )
    logger.info ( 'Collisions          : %s' % collisions         )
    logger.info ( 'Magnet polarities   : %s' % polarity           )
    logger.info ( 'Version             : %s' % config.version     )
    logger.info ( 'Particles           : %s' % list ( particles ) )
    logger.info ( 80 * '*')

    #
    ## Load PID samples
    #
    if config.TestPath:
        try:
            from grid import BKRequest, filesFromBK, hasGridProxy
        except ImportError:
            logger.error(
                "Can't import from ``grid'': for ``testpath'' one needs to use Bender"
            )
            return {}
        if not hasGridProxy():
            logger.error("Valid GRID proxy is required!")
            return {}
        path = config.TestPath
        logger.info('Test data to be loaded from %s' % path)
        data = load_from_grid ( path, particles, maxfiles = config.MaxFiles, verbose = config.verbose )

    elif config.TestFiles:
        logger.info ( 'Test files to be loaded from %s' % config.TestFiles)
        data = load_data (
            config.TestFiles           ,
            particles                  ,
            tag      = 'TESTFILES'     ,
            maxfiles = config.MaxFiles ,
            verbose  = config.verbose  )
    else:
        data = load_samples (
            particles                    ,
            years      = year            ,
            collisions = collisions      ,
            polarity   = polarity        ,
            version    = config.version  ,
            verbose    = config.verbose  ,
            maxfiles   = config.MaxFiles ,
            use_eos    = config.UseEos   )
    #
    ## Start processing
    #
    if config.Dump :
        config.verbose = True
        
        files = set()
        for k in data :
            for f in data [ k ] . files :
                files.add ( f )

        ss = set()
        from collections import defaultdict
        sk = defaultdict ( list )

        import ostap.core.pyrouts
        import ostap.trees.trees
        import ostap.io.root_file 
        
        for f in files:
            ff   = ROOT.TFile.Open ( f , 'READ' )
            keys = ff.keys()
            for k in keys:
                p = k.find ( '/DecayTree' )
                if 0 < p: ss.add ( k [ : p ] )
            ff.Close()

        for s in ss:
            s = s.replace ( 'Tuple' , '' )
            a, b, q = s.rpartition('_')
            if   q in ( 'PiP' , 'PiM' ) : sk [ 'PIONS'    ] . append ( s )
            elif q in ( 'MuP' , 'MuM' ) : sk [ 'MUONS'    ] . append ( s )
            elif q in ( 'KP'  , 'KM'  ) : sk [ 'KAONS'    ] . append ( s )
            elif q in ( 'P'   , 'Pbar') : sk [ 'PROTONS'  ] . append ( s )
            elif q in ( 'EP'  , 'EM'  ) : sk [ 'ELECTRONS'] . append ( s )
            else:
                sk [ 'OTHER' ] . append ( s )

        for k in sk :sk[k].sort()
            
        keys   = sk.keys()
        keys.sort()
        smpls1 = defaultdict ( int )
        smpls2 = defaultdict ( int )
        
        import ostap.logger.table as T
        
        for f in files :
            with ROOT.TFile.Open ( f , 'READ' ) as ff : 
                for k in keys :
                    sk[k].sort()
                    for s in sk[k] : 
                        c = ff.get ( s + 'Tuple/DecayTree', None )
                        if not c : continue
                        smpls1 [ s ] += len ( c ) 
                        smpls2 [ s ] += 1 


        table = [ ( '' , '#files' , '#events' ) ]
        for k in sorted ( sk  ) :
            if k == 'OTHER' : continue 
            for s in sk[k] :
                row = s , smpls2 [ s ] , smpls1 [ s ]
                table.append ( row )
        title = 'Found samples'
        table = T.table ( table , title = title , prefix = '# ' )
        logger.info ( '%s:\n%s' % ( title , table ) )
                                          
        tunes = set()  
        for k in data :
            branches = data[k].chain.branches() 
            for b in branches :
                for s in ( 'probe_Brunel_', 'probe_' ):
                    ns = len ( s ) 
                    if b.startswith ( s ) :
                        i = b.find ( '_ProbNN' , ns )
                        if ns < i:
                            tunes.add ( b [ ns : i ] )
        if tunes:
            logger.info ( 'Available PROBNN-tunes are %s' % list ( tunes ) )

            
        from  ostap.logger.colorized import allright, attention 
        table = [ ( 'Sample' , '#files' , '#events' , '      sWeight' ,
                    '  mean +/- rms   ' , '   min / max   ' ) ]
        
        for k in sorted ( data )  :
            chain = data[k].chain

            statvars = chain.statVars ( [ 'probe_sWeight', 'probe_Brunel_sWeight'] )
            
            for sv in statvars : 
                c   =  statvars[sv]
                cv  = c.values()
                rms = c.rms()
                
                csv = sv 
                c1  = "%+6.3f +- %-6.3f" % ( c .mean () , c .rms () ) 
                c2  = "%+6.2f / %-+6.2f" % ( cv.min  () , cv.max () ) 
                
                if 0 < rms and cv.min() < cv.max() : pass
                else : 
                    c2  = attention ( c2  )
                                                        
                row = k                             ,\
                      str ( len ( chain.files() ) ) ,\
                      str ( len ( chain )         ) ,\
                      csv , c1 , c2 

                table.append ( row ) 

        table = T.table ( table , title = 'sWeigth for selected samples' , prefix = '# ')
        logger.info ( 'sWeight for selected samples :\n%s' % table )
        
        return data 


    keys  = data.keys()
    keys.sort()

    ## check for triviality 
    for k in keys :
        d     = data [ k ]
        ## small chain
        sm = d.chain[:1]
        c1 = sm.statVar ( 'probe_sWeight'        )
        c2 = sm.statVar ( 'probe_Brunel_sWeight' )
        if 0 == c1.rms () :
            v1 = c1.values()
            logger.warning( "``probe_sWeight''        variable is trivial: mean/(min,max)=%s/(%s,%s)"
                            % ( c1.mean() , v1.min() , v1.max() ) )
        if 0 == c2.rms () :
            v2 = c2.values()
            logger.warning( "``probe_Brunel_sWeight'' variable is trivial: mean/(min,max)=%s/(%s,%s)"
                            % ( c2.mean() , v2.min() , v2.max() ) )

    # =========================================================================
    
    fun     = FUNC( config.cuts )
    tacc    = None
    trej    = None
    files   = set()    
    results = {}

    ## parapllel processing 
    if config.Parallel :
        
        task = PidCalibTask  ( fun            )
        
        from ostap.parallel.parallel import WorkManager 
        wmgr = WorkManager   ( silent = False )
        lst = []
        for k in data :
            chain = data[k].chain
            name  = chain.name 
            for f in chain.files() :
                row   = k , name , (f,)
                lst.append ( row )                 
        wmgr.process ( task , lst )

        results = task.results()

    ## sequential processing 
    else :
        
        ikey    = 0 
        for k in keys :
            
            ikey += 1  
            logger.info ( 'Processing %s [%2d/%-2d]' % ( k , ikey , len ( keys ) ) )
            
            d         = data [ k ]
            acc , rej = fun.run ( d.chain )
            results [ k ] = acc.clone() , rej.clone() 


    # =========================================================================
    keys = results.keys()
    keys.sort()

    processed = set()
    report  = [ ( 'Sample' , '#accepted [10^3]' , '#rejected [10^3]' , '<eff> [%]'  , '<eff> [%]' , 'min [%]' , 'max [%]' ) ]

    for k in keys:

        acc , rej = results [ k ]
        
        na = acc.accumulate () / 1000
        nr = rej.accumulate () / 1000

        heff = 100. / (1. + rej / acc )
        eeff = 100. / (1. + nr  / na  )
        hst  = heff.stat()
        del heff

        row = k , \
              na.toString   ( '%9.1f +- %-5.1f' ) ,            \
              nr.toString   ( '%9.1f +- %-5.1f' ) ,            \
              '%6.2f +- %-5.2f' % ( hst.mean() , hst.rms() ) , \
              eeff.toString ( '%6.2f +- %-5.2f' ) ,            \
              '%+7.1f'  % hst.min()               ,            \
              '%+7.1f'  % hst.max()
        
        report.append ( row )
        
        if    tacc : tacc += acc
        else       : tacc = acc.clone()

        if    trej : trej += rej
        else       : trej = rej.clone()

        for f in d.files: files.add ( f )

        processed.add ( k )
        import datetime
        now = datetime.datetime.now()
        with DBASE.open ( config.output ) as db:
            db [  k                                     ] = acc, rej
            db [  k + ':data'                           ] = d
            db [  k + ':created'                        ] = now
            db [  k + ':conf'                           ] = config
            db [ 'TOTAL_%s'           % config.particle ] = tacc, trej
            db [ 'TOTAL_%s:keys'      % config.particle ] = keys
            db [ 'TOTAL_%s:files'     % config.particle ] = files
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
                      na.toString   ( '%9.1f +- %-5.1f' ) ,            \
                      nr.toString   ( '%9.1f +- %-5.1f' ) ,            \
                      '%6.2f +- %-5.2f' % ( hst.mean() , hst.rms() ) , \
                      eeff.toString ( '%6.2f +- %-5.2f' ) ,            \
                      '%+7.1f'  % hst.min()               ,            \
                      '%+7.1f'  % hst.max()
                report.append ( row )
                
                logger.info('Output DBASE with results: %s' % config.output )
                db.ls ()
            except:
                pass


    import ostap.logger.table as T
    table = T.table ( report , title = 'Processed %s samples  %s ' % ( len ( keys ) , particles ) , prefix = '# ')
    logger.info ( 'Processed %s samples:\n%s' % ( len ( keys ) , table ) ) 
    
    logger.info ( 'Processed: Data taking periods : %s' % year               )
    logger.info ( 'Processed: Collisions          : %s' % collisions         )
    logger.info ( 'Processed: Magnet polarities   : %s' % polarity           )
    logger.info ( 'Processed: Version             : %s' % config.version     )
    logger.info ( 'Processed: Particles           : %s' % list ( particles ) )
    logger.info ( 'Processed: Keys                : %s' % keys               )

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
    def __init__ ( self , accepted , rejected , cuts = '' ) :
        
        self.__accepted = str ( ROOT.TCut ( accepted ) )
        self.__rejected = str ( ROOT.TCut ( rejected ) )
        self.__cuts     = str ( ROOT.TCut ( cuts     ) ) 

        if self.cuts:  ## redefine accepted/rejected
            self.__accepted = str ( ROOT.TCut ( self.cuts ) * ROOT.TCut ( self.accepted ) ) 
            self.__rejected = str ( ROOT.TCut ( self.cuts ) * ROOT.TCut ( self.rejected ) ) 
            logger.info ( "CUTS    : %s" % self.cuts )
            
        logger.info ( "ACCEPTED: %s" % self.accepted )
        logger.info ( "REJECTED: %s" % self.rejected )

    ## the  function :-)
    def __call__(self, data):
        return self.run ( data )

    # =========================================================================
    ## Abstract method: process the data
    #  @param data input data <code>TChain</code>
    #  @return tuple of histograms  with "accepted" and "rejected" distributions
    @abc.abstractmethod
    def run ( self , data ) :
        """Abstract method: process the data
        - `data` : input data `TChain`
        - return tuple of histograms  with ``accepted'' and ``rejected'' distributions
        """
        return None, None
    
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
    def __init__ ( self             ,
                   accepted         , ## accepted sample
                   rejected         , ## rejected sample
                   xbins            , ## bins in 1st axis
                   cuts     = None  , ## additional cuts (if any)
                   ## "Accept"-function                              what  to project/draw                                 cuts&weight
                   acc_fun  = lambda s,data : data.pproject ( s.ha , 'probe_Brunel_P/1000 ', '(%s)*probe_sWeight' % s.accepted , silent = True ) ,
                   ## "Reject"-function                              what  to project/draw                                 cuts&weight
                   rej_fun  = lambda s,data : data.pproject ( s.hr , 'probe_Brunel_P/1000 ', '(%s)*probe_sWeight' % s.rejected , silent = True ) ) :
        #
        ## the heart of the whole game:   DEFINE PID CUTS!
        #
        PARTICLE.__init__ (  self , accepted , rejected , cuts )
        
        #
        ## book 1D-histograms
        #
        import ROOT
        import ostap.core.pyrouts  
        from   ostap.histos.histos import h1_axis

        self.ha = h1_axis ( xbins , title='Accepted(%s)' % self.accepted )
        self.hr = h1_axis ( xbins , title='Rejected(%s)' % self.rejected ) 

        self.acc_fun = acc_fun
        self.rej_fun = rej_fun

    # =========================================================================
    ## The actual function to fill PIDCalib histograms
    def run ( self , data):
        """The actual function to fill PIDCalib histograms
        - it fills histograms with 'accepted' and 'rejected' events
        - ``data'' is a tree/chain from PIDCalib
        """
        
        #
        ## we need here ROOT and Ostap machinery!
        #
        self.ha = self.ha.clone()
        self.hr = self.hr.clone()
        
        na , ha = self.acc_fun ( self , data )
        nr , hr = self.rej_fun ( self , data )

        logger.debug ( 'Accepted/rejected entries: %ld/%ld' % ( na , nr ) )

        ha . SetName ( "Accepted" )
        hr . SetName ( "Rejected" )

        return ha , hr

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
    def __init__ ( self             ,
                   accepted         , ## accepted sample
                   rejected         , ## rejected sample
                   xbins            , ## bins in 1st axis
                   ybins            , ## bins in 2nd axis
                   cuts     = None  , ## additional cuts (if any)
                   ## "Accept"-function                              what  to project/draw                                 cuts&weight
                   acc_fun  = lambda s,data : data.pproject ( s.ha , 'probe_Brunel_ETA : probe_Brunel_P/1000 ', '(%s)*probe_sWeight' % s.accepted , silent = True ) ,
                   ## "Reject"-function                              what  to project/draw                                 cuts&weight
                   rej_fun  = lambda s,data : data.pproject ( s.hr , 'probe_Brunel_ETA : probe_Brunel_P/1000 ', '(%s)*probe_sWeight' % s.rejected , silent = True ) ) :
        #
        ## initialize the base class
        #
        PARTICLE_1D.__init__ ( self     ,
                               accepted ,
                               rejected ,
                               xbins    ,
                               cuts     ,
                               acc_fun  ,
                               rej_fun  )
        #
        ## book 2D-histograms
        #
        import ROOT
        import ostap.core.pyrouts
        from   ostap.histos.histos import h2_axes

        self.ha = h2_axes ( xbins , ybins , title = 'Accepted(%s)' % self.accepted )
        self.hr = h2_axes ( xbins , ybins , title = 'Rejected(%s)' % self.rejected )

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
    def __init__ ( self             ,
                   accepted         , ## accepted sample
                   rejected         , ## rejected sample
                   xbins            , ## bins in 1st axis
                   ybins            , ## bins in 2nd axis
                   zbins            , ## bins in 3rd axis
                   cuts     = None  , ## additional cuts (if any)
                   ## "Accept"-function                              what  to project/draw                                 cuts&weight
                   acc_fun  = lambda s,data : data.pproject ( s.ha , 'nTracks_Brunel : probe_Brunel_ETA : probe_Brunel_P/1000 ', '(%s)*probe_sWeight' % s.accepted , silent = True ) ,
                   ## "Reject"-function                              what  to project/draw                                 cuts&weight
                   rej_fun  = lambda s,data : data.pproject ( s.hr , 'nTracks_Brunel : probe_Brunel_ETA : probe_Brunel_P/1000 ', '(%s)*probe_sWeight' % s.rejected , silent = True ) ) :

        ## initialize the base class
        PARTICLE_2D.__init__ ( self     ,
                               accepted ,
                               rejected ,
                               xbins    ,
                               ybins    ,
                               cuts     ,
                               acc_fun  ,
                               rej_fun  )

        import ROOT
        import ostap.core.pyrouts
        from   ostap.histos.histos import h3_axes

        self.ha = h3_axes ( xbins , ybins , zbins , title = 'Accepted(%s)' % accepted )
        self.hr = h3_axes ( xbins , ybins , zbins , title = 'Rejected(%s)' % rejected )


# =============================================================================
if '__main__' == __name__ :
    
    from ostap.utils.docme import docme
    docme ( __name__ , logger = logger )

    run_pid_calib ( None , [ '-h'] ) 
# =============================================================================
##                                                                      The END
# =============================================================================
