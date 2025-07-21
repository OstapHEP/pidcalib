#!/usr/bin/env python
# -*- coding: utf-8 -*-
# =============================================================================
## @file  pidgen.py
#  A tiny wrapper for Anton Poluektov's pidgen2 machinery
#  @see https://pypi.org/project/pidgen2
#  - It allows to add the new corrected variables directly into the input TTree/TChain
#
# *******************************************************************************
# *                                                                             *
# *    ooooooooo.    o8o        .o8    .oooooo.                                 * 
# *    `888   `Y88.  `"'       "888   d8P'  `Y8b                                * 
# *     888   .d88' oooo   .oooo888  888            .ooooo.  ooo. .oo.          *
# *     888ooo88P'  `888  d88' `888  888           d88' `88b `888P"Y88b         *
# *     888          888  888   888  888     ooooo 888ooo888  888   888         *
# *     888          888  888   888  `88.    .88'  888    .o  888   888         * 
# *    o888o        o888o `Y8bod88P"  `Y8bood8P'   `Y8bod8P' o888o o888o        *
# *                                                                             *
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
#  - PidGen:
#
#  Resample the certain calibration variable (e.g `MC15TuneV1_ProbNNpi')
#  for tracks with given pt/eta/#ntrack the using calibration sample `sample`
#  fo certain data-taking period `dataset`    
#
#  Usage is fairly trivial:
# 
#  @code
#
#  ## input MC data to be resampled 
#  data_2016u = ROOT.TChain ( ... )
#  data_2016u = ROOT.TChain ( ... )

#  requests = [
#  ##                            out-var     sample           dataset          cabration variable        pt in MeV        pseudorapidity  #tracks
#   ( data_2016u , [ ReSample ( 'pid_pi1' , 'pi_Dstar2Dpi' , 'MagUp_2016'   , 'MC15TuneV1_ProbNNpi' , 'pt_pion[0]*1000' , 'eta_pion[0]' , 'nTracks' ) , 
#                    ReSample ( 'pid_pi2' , 'pi_Dstar2Dpi' , 'MagUp_2016'   , 'MC15TuneV1_ProbNNpi' , 'pt_pion[1]*1000' , 'eta_pion[1]' , 'nTracks' ) ,
#                    ReSample ( 'pid_K'   , 'K_Dstar2Dpi'  , 'MagUp_2016'   , 'MC15TuneV1_ProbNNK'  , 'pt_kaon*1000'    , 'eta_kaon'    , 'nTracks' ) ] ,
#   ( data_2016d , [ ReSample ( 'pid_pi1' , 'pi_Dstar2Dpi' , 'MagDown_2016' , 'MC15TuneV1_ProbNNpi' , 'pt_pion[0]*1000' , 'eta_pion[0]' , 'nTracks' ) , 
#                    ReSample ( 'pid_pi2' , 'pi_Dstar2Dpi' , 'MagDown_2016' , 'MC15TuneV1_ProbNNpi' , 'pt_pion[1]*1000' , 'eta_pion[1]' , 'nTracks' ) ,
#                    ReSample ( 'pid_K'   , 'K_Dstar2Dpi'  , 'MagDown_2016' , 'MC15TuneV1_ProbNNK'  , 'pt_kaon*1000'    , 'eta_kaon'    , 'nTracks' ) ] ,
#  ]
#
#  pgen = PidGen ( ...  )
#  pgen.process  ( requests , progress = True , report = True , parallel = True , ... )
#
#  @encode
#
#  `PidGen` feeds the `pidgen2.resampler.create_resampler` function with following arguments 
#        - `sample`
#        - `dataset`
#        - `variable`
# 
#  All other arguments can be provided via `PidGen` constructor
#
#  -  PidCorr
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
#  Track multiplicity can be specified in four different ways :
#   - as a variable in the input TTree, e.g. `nTracks`
#   - as an expression in the input TTree, e.g. scaled verison `nTracks*1.15`
#   - as a 1D-historgam with #track sdistribitionis in data. The actual #track will be smapled from this histograms (ignoring x<0)
#   - as a sequence/containe of some non-negative numbers. he actual #track will be smapled from this histograms (ignoring x<0) 
# 
#  `PidCorr` feeds the `pidgen2.correct` function with following arguments 
#     - `sample`
#     - `dataset`
#     - `variable`
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
"""  A tiny wrapper for Anton Poluektov's pidgen2 machinery
- see https://pypi.org/project/pidgen2
- It allows to add the new corrected variables directly into the input TTree/TChain

  ******************************************************************************
  *                                                                            *
  *    ooooooooo.    o8o        .o8    .oooooo.                                * 
  *    `888   `Y88.  `"'       "888   d8P'  `Y8b                               * 
  *     888   .d88' oooo   .oooo888  888            .ooooo.  ooo. .oo.         *
  *     888ooo88P'  `888  d88' `888  888           d88' `88b `888P"Y88b        *
  *     888          888  888   888  888     ooooo 888ooo888  888   888        *
  *     888          888  888   888  `88.    .88'  888    .o  888   888        * 
  *    o888o        o888o `Y8bod88P"  `Y8bood8P'   `Y8bod8P' o888o o888o       *
  *                                                                            *
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

  - PidGen:

  Resample the certain calibration variable (e.g `MC15TuneV1_ProbNNpi')
  for tracks with given pt/eta/#ntrack the using calibration sample `sample`
  for certain data-taking period `dataset`    

  Usage is fairly trivial:

  ## input MC data to be resampled 

  >>> data_2016u = ROOT.TChain ( ... )
  >>> data_2016u = ROOT.TChain ( ... )

  >>> from pidcalib.pidgen import PidGen, ReSample

  >>> requests = [
  ##                                    out-var     sample           dataset          cabration variable        pt in MeV        pseudorapidity  #tracks
  >>> ... ( data_2016u , [ ReSample ( 'pid_pi1' , 'pi_Dstar2Dpi' , 'MagUp_2016'   , 'MC15TuneV1_ProbNNpi' , 'pt_pion[0]*1000' , 'eta_pion[0]' , 'nTracks' ) , 
  >>> ...                  ReSample ( 'pid_pi2' , 'pi_Dstar2Dpi' , 'MagUp_2016'   , 'MC15TuneV1_ProbNNpi' , 'pt_pion[1]*1000' , 'eta_pion[1]' , 'nTracks' ) ,
  >>> ...                  ReSample ( 'pid_K'   , 'K_Dstar2Dpi'  , 'MagUp_2016'   , 'MC15TuneV1_ProbNNK'  , 'pt_kaon*1000'    , 'eta_kaon'    , 'nTracks' ) ] ,
  >>> ... ( data_2016d , [ ReSample ( 'pid_pi1' , 'pi_Dstar2Dpi' , 'MagDown_2016' , 'MC15TuneV1_ProbNNpi' , 'pt_pion[0]*1000' , 'eta_pion[0]' , 'nTracks' ) , 
  >>> ...                  ReSample ( 'pid_pi2' , 'pi_Dstar2Dpi' , 'MagDown_2016' , 'MC15TuneV1_ProbNNpi' , 'pt_pion[1]*1000' , 'eta_pion[1]' , 'nTracks' ) ,
  >>> ...                  ReSample ( 'pid_K'   , 'K_Dstar2Dpi'  , 'MagDown_2016' , 'MC15TuneV1_ProbNNK'  , 'pt_kaon*1000'    , 'eta_kaon'    , 'nTracks' ) ] ,

  >>> pgen = PidGen ( ...  )
  >>> pgen.process     ( requests , progress = True , report = True , parallel = True )

  `PidGen` feeds the `pidgen2.resampler.create_resampler` function with following arguments 
        - `sample`
        - `dataset`
        - `variable`
 
  All other arguments can be provided via `PidGen` constructor

  -  PidCorr

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
    
  Track multiplicity can be specified in four different ways :
   - as a variable in the input TTree, e.g. `nTracks`
   - as an expression in the input TTree, e.g. scaled verison `nTracks*1.15`
   - as a 1D-historgam with #track sdistribitionis in data. The actual #track will be smapled from this histograms (ignoring x<0)
   - as a sequence/containe of some non-negative numbers. he actual #track will be smapled from this histograms (ignoring x<0) 
     
  `PidCorr` feeds the `pidgen2.correct` function with following arguments 
     - `sample`
     - `dataset`
     - `variable`

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
    'ReSample' , ## helper class to describe the elementary request for PidGen 
    'Correct'  , ## helper class to describe the elementary request for PidCorr
    'PidGen'   , ## run pidgen2 machinery 
    'PidCorr'  , ## run pidgen2 machinery 
)
# =============================================================================
from   collections              import namedtuple
from   ostap.core.meta_info     import ostap_info 
from   ostap.core.ostap_types   import sequence_types, string_types  
from   ostap.utils.basic        import typename
from   ostap.utils.progress_bar import progress_bar
from   ostap.parallel.task      import Task
import ostap.utils.cleanup      as     CU 
import ostap.trees.trees
import pidgen2, ROOT, numpy, random, re  
# =============================================================================
assert (3,0,0,4) <= ostap_info , "OStap versiopm *MUST* be >= 3.0.0.4!"
# =============================================================================
## Logging 
# =============================================================================
from   ostap.logger.logger import getLogger
logger = getLogger ( 'ostap.tools.pidgen' )
# =============================================================================
## (good) variables in the tree ?
def vars_in_tree ( tree , *variables ) :
    #
    for variable in variables :
        if not variable in tree and not tree.good_variables ( variable.replace ( ':,' , '' ) ) :
            logger.error ( "Variable `%s` is not in input dataset!" )
            return False
        ##
    return True 
# =============================================================================
## @class ReSample
#  Helper claass to describe the elementary request for pidgen2 processing 
ReSample = namedtuple ( 'ReSample'  , ( 'outvar'     , ## name of the output variable name 
                                        'sample'     , ## sample, e.g. 'pi_Dstar2Dpi' 
                                        'dataset'    , ## dataset e.g. 'MagUp_2016'
                                        'variable'   , ## calibration variable 'MC12TuneV1_ProbNNK'
                                        'pt'         , ## how ot access pt in MeV 
                                        'eta'        , ## how to access eta 
                                        'ntrk'     ) ) ## how to access #tracks
# =============================================================================
## @class Correct
#  Helper claass to describe the elementary request for PidCorprocessing 
Correct = namedtuple ( 'Correct'  , ( 'invar'    ,   ## input variable to be corrected     
                                      'outvar'   ,   ## corrected (output) variable 
                                      'sample'   ,   ## Calibration sample,  e.g. 'pi_Dstatt2Dpi'
                                      'dataset'  ,   ## Calibration dataset  e.g. 'MagDown_2012'
                                      'variable' ,   ## Calibration variable e.g.  'MC!2TuneV1_ProbNNpi'
                                      'pt'       ,   ## how to get pt in MeV 
                                      'eta'      ,   ## how to get eta 
                                      'ntrk'     ) ) ## how to get number of tracks )
# ===============================================================================
## @class PidBase
#  Helper base class for PidGen & PidCorr
#  It provided some purely technical methods
#  @see PidGen
#  @see PidCorr
class PidBase(object) :
    """ Helper base class for PidGen & PidCorr
    It provides some purely technical methods
    - see PidGen
    - see PidCorr
    """
    # =========================================================================
    ## arguments to be forwarded to pidgen2
    #  @see pidgen2.correct.correct
    #  @see pidgen2.resampler.create_resampler    
    def __init__ ( self , nTrk_name = 'nTracks' , **kwargs ) :
        """ Arguments to be forwarded to pidgen2
        - see pidgen2.correct.correct
        - see pidgen2.resampler.create_resampler    
        """
        kw = { 'verbose'   : -2                ,
               'nan'       : -1                ,
               'plot'      : False             ,
               'usecache'  : False             ,
               'kernel'    : ( "default" , 0 ) ,
              }
        kw.update ( kwargs )
        self.__nTrk_name = nTrk_name 
        self.__kwargs    = kw
        
        if not 'local_storage' in kwargs :
            dir1 = CU.CleanUp.tempdir ( prefix = 'ostap-PIDGEN2-templates-'    )
            self.__kwargs [ 'local_storage'    ] = dir1
            if self.verbose : logger.info ( 'Local    template storage : %s' % dir1 ) 
    
    # =========================================================================
    ## the name of new/resampled `ntrk`  branch
    @property
    def nTrk_name ( self ) :
        """`ntrk_name` : the name of new/resampled #ntrk branch 
        """
        return self.__nTrk_name 

    # =========================================================================
    ## verbose processing?
    @property
    def verbose ( self ) :
        """`verbose` : verbos eprocessing 
        """
        return self.__kwargs.get ( 'verbose' , false )
    
    # =========================================================================
    ## Arguments to be forwarded to pidgen2
    #  @see pidgen2.correct.correct
    #  @see pidgen2.resampler.create_resampler    
    @property
    def kwargs ( self ) : 
        """`kwargs` : Arguments to be forwarded to pidgen2
        - see pidgen2.correct.correct
        - see pidgen2.resampler.create_resampler    
        """
        return self.__kwargs
    
    # =========================================================================
    ## Helper method to check the type of requests
    #  - single request is convered into tuple of requests
    #  - all requests are requred to be of correct type
    @classmethod 
    def requests ( klass , requests ) :
        """ Helper class to check the type of requests
        - single request is convered into tuple of requests
        - all requests are requred to be of correct type 
        """
        if isinstance ( requests , klass.Request ) : requests = requests,
        assert isinstance ( requests , sequence_types ) and \
            all ( isinstance ( r , klass.Request ) for r in requests ) , \
            "Invalid `requests` type %s" % typename ( requests )
        return tuple ( requests )

    # ==========================================================================
    ## Check existence/validity of sample/dataset combination
    #  @see pidgen2.resampling.get_samples 
    @classmethod 
    def check_requests ( klass , requests , simversion  = '' ) :
        """ Check existence/validity  of sample/dataset combination         
        - see pidgen2.resampling.get_samples 
        """
        requests = klass.requests ( requests )
        ## 
        from pidgen2.resampling import get_samples 
        samples = get_samples()
        for r in requests :
            the_sample = samples.get ( r.sample , None )
            assert the_sample and r.dataset in the_sample , \
                "Invalid/non-existing sample/dataset: %s/%s" % ( r.sample , r.dataset )
            
        if simversion :
            from pidgen2.resampling import get_mc_samples 
            samples = get_mc_samples()
            for r in requests :
                the_sample = samples.get ( r.sample , None )
                ds = '%s_%s' % ( simversion , r.dataset )
                assert the_sample and ds in the_sample , \
                    "Invalid/non-existing simversion/sample/dataset: %s/%s/%s" % ( simversion , r.sample , r.dataset )
                
        return True
    
    # =========================================================================
    ## Check a single request
    def check_request ( self , tree , request ) :
        """ Check a single request
        """
        if not vars_in_tree ( tree , request.pt , request.eta ) : return False
        if request.outvar in tree :
            logger.error ( 'Variable %s already in the ROOT.TTree!' % r.outvar )
            return False

        if isinstance ( request.ntrk , string_types ) :
            return vars_in_tree ( tree , request.ntrk )

        if not self.good_for_sampling ( request.ntrk ) : 
            logger.error ( 'Wrong `ntrk` specification: %s ' % typename ( request.ntrk ) ) 
            return False

        if self.nTrk_name and self.nTrk_name in tree :
            logger.error ( 'Variable %s already in the ROOT.TTree!' % self.nTrk_name )
            return False 

        return True
    
    # =========================================================================
    ## Is the histogram good enough for #ntrk sampling ? 
    def sampling_histo ( self , histo ) :
        """ Is the histigram good for #ntrk sampling ? 
        """
        import ostap.histos.histos

        if not isinstance ( histo , ROOT.TH1 ) : return False
        if not 1 == histo.GetDimension()       : return False 
        xmin, xmax = histo.xminmax()
        if xmax < 100                          : return False  ## upper limit cannot be smaller than 100! 

        last_bin = len ( histo ) 
        zero_bin = 1 if 0 <= xmin else nmax ( 1 , histo.FindBin ( 0 ) )
        if last_bin <= zero_bin                 : return False
        #
        return  0 < histo.Integral ( zero_bin , last_bin )
    # =========================================================================
    ## Is the object good enough for #ntrk sampling ?
    #  - 1D-histo with specific setting 
    #  - sequence of non-negative numbers 
    def good_for_sampling ( self , obj  ) :
        """ Is the object good for #ntrk sampling ? 
        - 1D-histo with specific setting 
        - sequence of non-negative numbers 
        """
        if isinstance ( obj , ROOT.TH1 ) : return self.sampling_histo (  obj )
        ## otherwise some non-empty sequnce of non-negative numbers 
        return obj and 10 <= len ( obj ) and isinstance ( obj , sequence_types ) and all ( 0 < int ( v ) for v in obj )

    # =========================================================================
    ## Generate/sample #ntr array of length N 
    def sample_ntrk ( self , ntrk , N ) :
        """ Generate/sample #ntr array of length N 
        """
        if not self.good_for_sampling ( ntrk ) : return None
        ## 
        if self.sampling_histo ( ntrk ) :
            sample = tuple ( int ( v ) for v in ntrk.shoot     ( N , lambda s : 0 < s ) )
        else :
            sample = tuple ( int ( v ) for v in random.choices ( ntrk , k = N ) )
        
        return numpy.asarray ( sample , dtype = numpy.uint16 )
        
    # =========================================================================
    ## Get data from a single tree 
    def get_data ( self , tree , *vars , ntrk = None ) :
        """ Get data from a single tree 
        """
        ## number of entries
        if ntrk is None : 
            data , _ = tree.slice ( [ v for v in vars ] ,            structured = False , transpose = True ) 
        elif isinstance ( ntrk , string_types  ) and tree.good_variables ( ntrk ) :
            data , _ = tree.slice ( [ v for v in vars ] + [ ntrk ] , structured = False , transpose = True )
        elif isinstance ( ntrk , numpy.ndarray ) and len ( ntrk ) == len ( tree ) :
            data , _ = tree.slice ( [ v for v in vars ] ,            structured = False , transpose = True ) 
            data     = numpy.column_stack ( [ data , ntrk ] )
        else :
            raise  TypeError ( "get_data: invalid type  for `ntrk` : %s" % typename ( ntrk ) )

        return data
    
# =============================================================================
## @class PidGen
#  A tiny wrapper for Anton Poluektov's pidgen2 machinery
#  @see https://pypi.org/project/pidgen2
#  - It allows to add the new resampled variable directly into the input TTree/TChain
#
#  @code
#  requests = [
#   ( data_2015u , [ ReSample ( 'pid_pi1' , 'pi_Dstar2Dpi' , 'MagUp_2015'   , 'MC15TuneV1_ProbNNpi' , 'pt_pion[0]*1000' , 'eta_pion[0]' , 'nTracks' ) , 
#                    ReSample ( 'pid_pi2' , 'pi_Dstar2Dpi' , 'MagUp_2015'   , 'MC15TuneV1_ProbNNpi' , 'pt_pion[1]*1000' , 'eta_pion[1]' , 'nTracks' ) ,
#                    ReSample ( 'pid_K'   , 'K_Dstar2Dpi'  , 'MagUp_2015'   , 'MC15TuneV1_ProbNNK'  , 'pt_kaon*1000'    , 'eta_kaon'    , 'nTracks' ) ] ,
#   ( data_2015d , [ ReSample ( 'pid_pi1' , 'pi_Dstar2Dpi' , 'MagDown_2015' , 'MC15TuneV1_ProbNNpi' , 'pt_pion[0]*1000' , 'eta_pion[0]' , 'nTracks' ) , 
#                    ReSample ( 'pid_pi2' , 'pi_Dstar2Dpi' , 'MagDown_2015' , 'MC15TuneV1_ProbNNpi' , 'pt_pion[1]*1000' , 'eta_pion[1]' , 'nTracks' ) ,
#                    ReSample ( 'pid_K'   , 'K_Dstar2Dpi'  , 'MagDown_2015' , 'MC15TuneV1_ProbNNK'  , 'pt_kaon*1000'    , 'eta_kaon'    , 'nTracks' ) ] ,
#  ]
#  pgen = RunPidGen ( ...  )
#  pgen.process     ( requests , progress = True , .... )
#
#  @encode
#
#  Track multiplicity can be specified in three different ways ways :
#   - as a variable in the input TTree, e.g. `nTracks`
#   - as an expression in the input TTree, e.g. scaled verison `nTracks*1.15`
#   - as a 1D-historgam with #track sdistribitionis in data. The actual #track will be smapled from this histograms (ignoring x<0)
#   - as a sequence/containe of some non-negative numbers. he actual #track will be smapled from this histograms (ignoring x<0) 
#
#  `PidGen` feeds the `pidgen2.resampler.create_resampler` function with following arguments 
#        - `sample`
#        - `dataset`
#        - `variable`
# 
#  All other arguments can be provided via `PidCorr` constructor
#
#  IMPORTANT: It updates the input data, therefore
#   - One can *not* process the input dat via the READ-ONLY protocols
#   - CERN /eos is not reliable storage for modifications of dataL
#     ROOT tends to segfault when data in /eos/ are modified 
#
#  @author Vanya BELYAEV Ivan.Belyaev@cern.ch
#  @date   2025-07-12
class PidGen(PidBase):
    """ A tiny wrapper for Anton Poluektov's pidgen2 machinery
    - see https://pypi.org/project/pidgen2
    - It allows to add the new resampled variable directly into the input TTree/TChain
    
    >>> requests = [
    >>>  ...  ( data_2015u , [ ReSample ( 'pid_pi1' , 'pi_Dstar2Dpi' , 'MagUp_2015'   , 'MC15TuneV1_ProbNNpi' , 'pt_pion[0]*1000' , 'eta_pion[0]' , 'nTracks' ) , 
    >>>  ...                   ReSample ( 'pid_pi2' , 'pi_Dstar2Dpi' , 'MagUp_2015'   , 'MC15TuneV1_ProbNNpi' , 'pt_pion[1]*1000' , 'eta_pion[1]' , 'nTracks' ) ,
    >>>  ...                   ReSample ( 'pid_K'   , 'K_Dstar2Dpi'  , 'MagUp_2015'   , 'MC15TuneV1_ProbNNK'  , 'pt_kaon*1000'    , 'eta_kaon'    , 'nTracks' ) ] ,
    >>>  ...  ( data_2015d , [ ReSample ( 'pid_pi1' , 'pi_Dstar2Dpi' , 'MagDown_2015' , 'MC15TuneV1_ProbNNpi' , 'pt_pion[0]*1000' , 'eta_pion[0]' , 'nTracks' ) , 
    >>>  ...                   ReSample ( 'pid_pi2' , 'pi_Dstar2Dpi' , 'MagDown_2015' , 'MC15TuneV1_ProbNNpi' , 'pt_pion[1]*1000' , 'eta_pion[1]' , 'nTracks' ) ,
    >>>  ...                   ReSample ( 'pid_K'   , 'K_Dstar2Dpi'  , 'MagDown_2015' , 'MC15TuneV1_ProbNNK'  , 'pt_kaon*1000'    , 'eta_kaon'    , 'nTracks' ) ] ]
    >>> pgen = PidGen ( ... )
    >>> pgen.process ( requests , progress = True , .... ) 
    
    Track multiplicity can be specified in four different ways :
     - as a variable in the input TTree, e.g. `nTracks`
     - as an expression in the input TTree, e.g. scaled verison `nTracks*1.15`
     - as a 1D-historgam with #track sdistribitionis in data. The actual #track will be smapled from this histograms (ignoring x<0)
     - as a sequence/containe of some non-negative numbers. he actual #track will be smapled from this histograms (ignoring x<0) 
     
   `PidGen` feeds the `pidgen2.resampler.create_resampler` function with following arguments 
        - `sample`
        - `dataset`
        - `variable`
 
    All other arguments can be provided via `PidGen` constructor

    IMPORTANT: It updates the input data, therefore
    - One can *not* process the input dat via the READ-ONLY protocols
    - CERN /eos is not reliable storage for modifications of dataL
     ROOT tends to segfault when data in /eos/ are modified 
    
    """
    # =========================================================================
    ## The actual type for the elementary request 
    Request = ReSample                
    # ==========================================================================
    ## Create the resampler and run the pidgen2 machinery
    #  @param data (INPUT) input numpy array dimension of (N,3)
    #  @code
    #  pgen = PidGen ( ... )
    #  data = ... ## numpy array
    #  result, stat = pgen ( data ) 
    #  @endcode
    #  @see pidgen2.resampler
    #  @see pidgen2.resampler.create_resampler     
    def __call__ ( self     ,
                   data     ,
                   sample   ,
                   dataset  ,
                   variable ) :
        """ Create the resampler and run the pidgen2 machinery 
        >>> pgen = PidGen ( ... )
        >>> data = ... ## numpy array of dimension (N,3) 
        >>> result, stat = pgen ( data ) 
        - see pidgen2.resampler
        - see pidgen2.resampler.create_resampler     
        """
        assert isinstance  ( data , numpy.ndarray )               , "Invalid `data` type  %s"  % ( typename ( data ) )
        assert 2 == len ( data.shape ) and  3 == data.shape [ 1 ] , "Invalid `data` shape %s"  % str ( data.shape )

        ## use pidgen2 machinery!!! 
        from pidgen2.resampler import create_resampler
        resampler = create_resampler ( sample   = sample   ,
                                       dataset  = dataset  ,
                                       variable = variable , **self.kwargs )
        
        return resampler ( data ) 

    # =========================================================================
    ## The main entry point: run PidGen machinery for several variables/samples/datasets/trees
    #  @code
    #  requests = [
    #   ( data_2015u , [ ReSample ( 'pid_pi1' , 'pi_Dstar2Dpi' , 'MagUp_2015'   , 'MC15TuneV1_ProbNNpi' , 'pt_pion[0]*1000' , 'eta_pion[0]' , 'nTracks' ) , 
    #                    ReSample ( 'pid_pi2' , 'pi_Dstar2Dpi' , 'MagUp_2015'   , 'MC15TuneV1_ProbNNpi' , 'pt_pion[1]*1000' , 'eta_pion[1]' , 'nTracks' ) ,
    #                    ReSample ( 'pid_K'   , 'K_Dstar2Dpi'  , 'MagUp_2015'   , 'MC15TuneV1_ProbNNK'  , 'pt_kaon*1000'    , 'eta_kaon'    , 'nTracks' ) ] ,
    #   ( data_2015d , [ ReSample ( 'pid_pi1' , 'pi_Dstar2Dpi' , 'MagDown_2015' , 'MC15TuneV1_ProbNNpi' , 'pt_pion[0]*1000' , 'eta_pion[0]' , 'nTracks' ) , 
    #                    ReSample ( 'pid_pi2' , 'pi_Dstar2Dpi' , 'MagDown_2015' , 'MC15TuneV1_ProbNNpi' , 'pt_pion[1]*1000' , 'eta_pion[1]' , 'nTracks' ) ,
    #                    ReSample ( 'pid_K'   , 'K_Dstar2Dpi'  , 'MagDown_2015' , 'MC15TuneV1_ProbNNK'  , 'pt_kaon*1000'    , 'eta_kaon'    , 'nTracks' ) ] ,
    #  ]
    #  pgen = PidGen ( ... )
    #  pgen.pprocess ( requests  , progress = True , .... ) 
    #  @encode
    ## Process all requests 
    def process ( self             ,
                  the_requests     , * , 
                  progress = True  ,
                  report   = False ,
                  silent   = False ,
                  parallel = False , **kwargs ) : 
        """ The main entry point: run PidGen machinery for several variables/samples/datasets/trees
        >>> requests = [
        >>>  ...  ( data_2015u , [ ReSample ( 'pid_pi1' , 'pi_Dstar2Dpi' , 'MagUp_2015'   , 'MC15TuneV1_ProbNNpi' , 'pt_pion[0]*1000' , 'eta_pion[0]' , 'nTracks' ) , 
        >>>  ...                   ReSample ( 'pid_pi2' , 'pi_Dstar2Dpi' , 'MagUp_2015'   , 'MC15TuneV1_ProbNNpi' , 'pt_pion[1]*1000' , 'eta_pion[1]' , 'nTracks' ) ,
        >>>  ...                   ReSample ( 'pid_K'   , 'K_Dstar2Dpi'  , 'MagUp_2015'   , 'MC15TuneV1_ProbNNK'  , 'pt_kaon*1000'    , 'eta_kaon'    , 'nTracks' ) ] ,
        >>>  ...  ( data_2015d , [ ReSample ( 'pid_pi1' , 'pi_Dstar2Dpi' , 'MagDown_2015' , 'MC15TuneV1_ProbNNpi' , 'pt_pion[0]*1000' , 'eta_pion[0]' , 'nTracks' ) , 
        >>>  ...                   ReSample ( 'pid_pi2' , 'pi_Dstar2Dpi' , 'MagDown_2015' , 'MC15TuneV1_ProbNNpi' , 'pt_pion[1]*1000' , 'eta_pion[1]' , 'nTracks' ) ,
        >>>  ...                   ReSample ( 'pid_K'   , 'K_Dstar2Dpi'  , 'MagDown_2015' , 'MC15TuneV1_ProbNNK'  , 'pt_kaon*1000'    , 'eta_kaon'    , 'nTracks' ) ] ]
        >>> pgen = PidGen ( ... )
        >>> pgen.process ( requests , progress = True , .... ) 
        """

        ## (1) initial loop over the entries
        printed = False 
        for tree , requests in the_requests  :
            assert isinstance ( tree , ROOT.TTree ) , "Invalid type for `tree` %s" % typename ( tree )
            reqs = self.requests ( requests )
            assert self.check_requests ( reqs ) , "Invalid/non-exising requests!"
            ## more check for requests, this time  one-by-one 
            if any ( not self.check_request ( tree , r ) for r in requests ) : return 
            for f in tree.files :
                if not printed and '/eos/' in f :
                     logger.warning ( 'EOS is *NOT* a reliable storage for safe modification of data!' )
                     printed = True
                    
                        
        NR             = len ( the_requests )        
        local_progress = progress and 10 <= NR 
        down_progress  = progress and 10 >  NR 

        ## (2) start pidgen processing
        nr = 1  
        for tree , requests in progress_bar ( the_requests , silent = not local_progress , description = 'Input:') :
            if not local_progress and not silent : logger.info ( 'Processing bunch #%d from %d' % ( nr , NR ) ) 
            nr += 1 
            requests = self.requests ( requests ) 
            self.run ( tree     ,
                       requests , 
                       progress = down_progress ,
                       report   = report        ,
                       silent   = silent or local_progress , 
                       parallel = parallel      , **kwargs )

    # =========================================================================
    ## The secondary entry point: Run pidgen machinery for several request for given tree/chain 
    #  @param tree    (INPUT/UPDATE) the input/update TTree/TChain
    #  @param requests (INPUT)       the list of elementary requests
    #  @see ReSample 
    def run ( self             ,
              tree             ,     ## input TTree/TChain
              requests         , * , ## requests to process
              progress = True  ,     ## show progress ?
              report   = True  ,     ## make a report ?
              silent   = False ,    
              parallel = False , 
              **kwargs         ) :
        """ The secondary entry point: run pidgen machinery for several request for given tree/chain 
        - tree    (INPUT/UPDATE) the input/update TTree/TChain
        - requests (INPUT)       the olist of elementary requests
        """
        ## (1) check input data 
        assert isinstance ( tree , ROOT.TTree ) , "Invalid `tree` type: %s" % typename ( tree ) 
        
        ## (2) check the configuration of requests
        requests = self.requests ( requests )

        ## (3)  more checks the requests
        for r in requests :
            if r.outvar in tree :
                logger.error ( 'Variable %s already in the ROOT.TTree, skip processing!' % r.outvar )
                return tree
            elif isinstance ( r.ntrk, string_types ) : pass
            elif self.good_for_sampling ( r.ntrk ) and self.nTrk_name and self.nTrk_name in tree :
                logger.error ( 'Variable %s already in the ROOT.TTree, skip processing!' % self.nTrk_name )
                return                                
                        
        ## (4) chain processing ?
        if isinstance ( tree , ROOT.TChain ) and 1 < tree.nFiles :
            return self.__run_chain ( tree     ,
                                      requests , 
                                      progress = progress ,
                                      report   = report   ,
                                      silent   = silent   , 
                                      parallel = parallel , **kwargs )

        the_file = tree.files [ 0 ]
        
        ## number of entries 
        N = len ( tree )

        ## sampled #tracks 
        sampled_ntrk = None
        
        ## some tree utilities 
        from ostap.trees.cuts  import vars_and_cuts, expression_types 

        results = {}
        
        ## explicit loop over all input requests 
        for request in progress_bar ( requests , silent = not progress , description = 'Requests:' ) :
            
            if not progress and not silent :
                logger.info ( 'Processing request: variable/sample/dataset : %s/%s %s' % ( request.variable ,
                                                                                           request.sample   ,
                                                                                           request.dataset  ) ) 
            ## recipes to access pt, eta and ntrk variables 
            pt , eta , ntrk = request.pt , request.eta , request.ntrk
            
            ## sample ntrk if requested
            if sampled_ntrk is None and not isinstance ( ntrk , string_types ) :
                sampled_ntrk = self.sample_ntrk ( ntrk , N = N )
                
            ## get the data 
            data = self.get_data ( tree , pt , eta , ntrk = ntrk if sampled_ntrk is None else sampled_ntrk )
            
            ## run the actual pidgen machinery 
            pids , _ = self ( data             ,
                              request.sample   ,
                              request.dataset  ,
                              request.variable ) 

            ## collect the results 
            results [ request.outvar ] = pids 

        
        ## addd #ntrk is requested
        if not sampled_ntrk is None and self.nTrk_name :
            results [ self.nTrk_name ] = sampled_ntrk 
            
        ## add result to TTree:
        return tree.add_new_buffer ( results , report = report , progress = progress )

    
    # =========================================================================
    ## Internal function to run pidgen machinery over TChain with many files
    #  @param chain    (INPUT/UPDATE) input/update TTtree/TChain
    #  @param requests (INPUT)       the list of elementary requests    
    def __run_chain ( self             ,
                      chain            ,     ## input TTree/TChain
                      requests         , * , ## requests  
                      progress = True  ,     ## show progress
                      report   = True  ,     ## make a report ? 
                      parallel = False ,    ## use the parallel processing ?  
                      silent   = False , 
                      **kwargs         ) : 
        """ Internal function to run pidgen machinery over TChain with many files 
        - chain    (INPUT/UPDATE) input/update TTtree/TChain
        - requests (INPUT)        the list of elementary requests    
        """
        
        ## (1) check the configuration of requests
        requests = self.requests ( requests )

        ## (2) treat the input chain/tree 
        assert isinstance ( chain  , ROOT.TTree ) , "Invalid type of `chain`: %s" % typename ( chain )

        ## (3) check request
        for r in requests :
            if r.outvar in chain :
                logger.error ( 'Variable %s already in the TTree, skip processing!' % r.outvar )
                return chain 

        ## (4) simple tree ?
        if not isinstance ( chain  , ROOT.TChain ) or 2 > chain .nFiles  :
            return self.run ( chain    ,
                              requests , 
                              progress = progress ,
                              report   = report   , **kwargs )
        
        # ========================================================================================
        ## list of existing branches/leaves 
        branches = ( set ( chain.branches() ) | set ( chain.leaves() ) ) if report else set() 

        ## chain name 
        cname = chain.fullpath 
        ## files to be processed 
        files = chain.files

        ## parallel processing? 
        if parallel and files :
        
            ## create the tast for the paralell processing
            task  = PidTask ( self , requests )
            
            from ostap.trees.utils import Chain
            ch    = Chain    ( chain )    
            trees = ch.split ( chunk_size = -1 , max_files = 1  )
    
            ## Manager
            from   ostap.parallel.parallel import WorkManager
            wmgr   = WorkManager ( silent = silent , progress = progress , **kwargs )
            wmgr.process ( task , trees )

        else :
            ## sequential processing here :

            NR             = len ( files  )
            local_progress = progress and 10 <= NR 
            down_progress  = progress and 10 >  NR
            
            for fname in progress_bar ( files , silent = not local_progress , description = 'Files:' ) :
                if not local_progress and not silent : logger.info ( "Processing file: %s" % fname ) 
                tree = ROOT.TChain ( cname )
                tree.Add ( fname ) 
                ## treat the tree 
                self.run ( tree     ,
                           requests , 
                           progress = down_progress            ,
                           silent   = silent or local_progress , 
                           report   = False         , **kwargs ) 

        ## reconstruct the resulting chain 
        chain = ROOT.TChain ( cname )
        for fname in files : chain.Add ( fname )
        
        if report :        
            new_branches = sorted ( ( set ( chain.branches () ) | set ( chain.leaves () ) ) - branches )
            if new_branches :
                n = len ( new_branches )
                if 1 >= n : title = "Added %s branch to TChain(%s)"   % ( n , cname ) 
                else      : title = "Added %s branches to TChain(%s)" % ( n , cname ) 
                table = chain.table ( new_branches , title = title , prefix = '# ' )
                logger.info ( '%s:\n%s' % ( title , table ) ) 
                chain = ROOT.TChain ( cname )
                for fname in files : chain.Add ( fname )
                
        return chain 

# =============================================================================
## @class PidCorr
#  A tiny wrapper for Anton Poluektov's pidgen2 machinery
#  @see https://pypi.org/project/pidgen2
#  - It allows to add the new resampled variable directly into the input TTree/TChain#
# 
# 
#  The usage is fairly transparent:
#
#  @code
#
#  ## input MC data to be corrected 
#  data_2016u = ROOT.TChain ( ... )
#  data_2016u = ROOT.TChain ( ... )
#
#  from pidcalib.pidcorr import PidCorr, Correct
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
#  Track multiplicity can be specified in three different ways:
#   - as a variable in the input TTree, e.g. `nTracks`
#   - as an expression in the input TTree, e.g. scaled verison `nTracks*1.15`
#   - as a 1D-historgam with #track sdistribitionis in data. The actual #track will be smapled from this histograms (ignoring x<0)
#   - as a sequence/containe of some non-negative numbers. he actual #track will be smapled from this histograms (ignoring x<0) 
#  @endcode
# 
#  IMPORTANT: It updates the input data, therefore
#   - One can *not* process the input dat via the READ-ONLY protocols
#   - CERN /eos is not reliable storage for modifications of dataL
#     ROOT tends to segfault when data in /eos/ are modified 
#
#  @author Vanya BELYAEV Ivan.Belyaev@cern.ch
#  @date   2025-07-12
class PidCorr(PidBase) :
    """  A tiny wrapper for Anton Poluektov's pidgen2/pidcorr machinery
    - see https://pypi.org/project/pidgen2
    - It allows to add the new resampled variables directly into the input TTree/TChain
    
    The usage is fairly transparent:
    
    ## input MC-data to be corrected 
    
    >>> data_2016u = ROOT.TChain ( ... )
    >>> data_2016u = ROOT.TChain ( ... )
    
    >>> from pidcalib.pidcorr import PidCorr, Correct
    
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
    
    For input data-chais one cna create TChani manualy (as shown above) or 
    using ostap.trees.data.Data utility 
    
    Track multiplicity can be specified in four different ways :
     - as a variable in the input TTree, e.g. `nTracks`
     - as an expression in the input TTree, e.g. scaled verison `nTracks*1.15`
     - as a 1D-historgam with #track sdistribitionis in data. The actual #track will be smapled from this histograms (ignoring x<0)
     - as a sequence/containe of some non-negative numbers. he actual #track will be smapled from this histograms (ignoring x<0) 
     
    `PidCorr` feeds the `pidgen2.correct` function with following arguments 
        - `sample`
        - `dataset`
        - `variable`

    All other arguments can be provided via `PidCorr` constructor
    
    IMPORTANT: It updates the input data, therefore
    - One can *not* process the input dat via the READ-ONLY protocols
    - CERN /eos is not reliable storage for modifications of dataL
     ROOT tends to segfault when data in /eos/ are modified 

    """
    # =========================================================================
    ## The actual type for the elementary request 
    Request = Correct 
    # =========================================================================
    ## constructor
    # `PidCorr` feeds the `pidgen2.correct` function with following arguments 
    # - `input` 
    # - `sample`
    # - `dataset`
    # - `variable`
    # - `branches`
    # - `output`
    # - `outtree` 
    # - `pidcorr`
    # - `friend` 
    # 
    # All other arguments can be provide via `PidCorr` constructor    
    def __init__ ( self       ,
                   simversion , **kwargs ) :
        """ `PidCorr` feeds the `pidgen2.correct` function with following arguments 
        - `input` 
        - `sample`
        - `dataset`
        - `branches`
        - `output`
        - `outtree` 
        - `pidcorr`
        - `friend` 

        All other arguments can be provide via `PidCorr` constructor
        """
        kw = { 'simversion'  : simversion }
        kw.update ( kwargs )
        dir2 = ''
        if not 'local_mc_storage' in kw :
            dir2 = CU.CleanUp.tempdir ( prefix = 'ostap-PIDGEN2-mc_templates-' )
            kw [ 'local_mc_storage' ] = dir2            
        super().__init__ ( **kw )
        ## 
        if dir2 and self.verbose : logger.info ( 'Local mc_template storage : %s' % dir2 ) 

    # =========================================================================
    ## Create the correctie and run the pidgen2 machinery
    #  @param data (INPUT) input numpy array dimension of (N,3)
    #  @code
    #  pgen = PidCorr ( ... )
    #  data = ... ## numpy array
    #  result, stat = pgen ( data ) 
    #  @endcode
    #  @see pidgen2.corrector
    #  @see pidgen2.corrector.create_corrector
    def __call__ ( self     ,
                   data     ,
                   sample   ,
                   dataset  ,
                   variable ) :
        """ Create the resampler and run the pidgen2 machinery 
        >>> pgen = PidGen ( ... )
        >>> data = ... ## numpy array of dimension (N,3) 
        >>> result, stat = pgen ( data ) 
        - see pidgen2.corrector
        - see pidgen2.corrector.create_corrector     
        """
        assert isinstance ( data , numpy.ndarray )               , "Invalid `data` type  %s"  % ( typename ( data ) )
        assert 2 == len ( data.shape ) and 4 == data.shape [ 1 ] , "Invalid `data` shape %s"  % str ( data.shape )

        ## use pidgen2 machinery!!! 
        from pidgen2.corrector import create_corrector
        corrector = create_corrector ( sample   = sample   ,
                                       dataset  = dataset  ,
                                       variable = variable , **self.kwargs )
        return corrector ( data ) 

    # ========================================================================================
    ## The main entry point: processing of  input data in a form of (chain, [requests] ) pairs
    #  @param the_requests    (INPUT)  the sequence (chain,[requests]) pairs    
    def process ( self                 ,
                  the_requests         , * , 
                  progress     = False ,
                  report       = False ,
                  silent       = False ,                   
                  parallel     = False , **kwargs ) :
        """ The main entry point: processing of  input data in a form of (chain, [requests] ) pairs
        - the_requests    (INPUT)  the sequence (chain,[requests]) pairs    
        """
        
        ## (1) initial loop over the entries
        printed = False 
        for tree , requests in the_requests  :
            assert isinstance ( tree , ROOT.TTree ) , "Invalid type for `tree` %s" % typename ( tree )
            reqs = self.requests ( requests )
            assert self.check_requests ( reqs , self.kwargs.get ( 'simversion', '' ) ) , "Invalid/non-exising requests!"
            ## more check for requests, this time  one-by-one 
            if any ( not self.check_request ( tree , r ) for r in requests ) : return
            if any ( not vars_in_tree ( tree , r.invar ) for r in requests ) : return 
                
            ## more checks..
            outvars = set()
            for r in reqs :
                if r.outvar in outvars :
                    logger.error ( 'Variable %s defined twice!' % r.outvar )
                    return                                                           ## RETURN                
                outvars.add ( r.outvar )
                     
            for f in tree.files :
                if not printed and '/eos/' in f :
                     logger.warning ( 'EOS is *NOT* a reliable storage for safe modification of data!' )
                     printed = True

                    
        NR              = len ( the_requests )        
        local_progress  = progress and 10 <= NR 
        down_progress   = progress and 10 >  NR 
        
        ## loop over input data
        nr = 1 
        for tree , requests in progress_bar ( the_requests , silent = not local_progress , description = "Input:" ) :
            if not local_progress and not silent : logger.info ( 'Processing bunch #%d from %d' % ( nr , NR ) )
            nr += 1 
            self.run ( tree     ,
                       requests = requests      ,
                       progress = down_progress ,
                       report   = report        , 
                       silent   = silent or local_progress , 
                       parallel = parallel      , **kwargs ) 
            
    # =========================================================================
    ## The minimal action: process a single  input file
    #  @param tree      (INPUT/UPDATE)  input/update TTree 
    #  @param requests  (INPUT)         the list of requests/actions 
    def run ( self              ,
              tree              , 
              requests          , * , 
              progress  = False ,
              report    = False ,
              silent    = False , 
              parallel  = False , **kwargs ) :
        """ The minimal action: process a single  input file
        - file_tree (INPUT/UPDATE)  input/update TTree 
        - requests  (INPUT)         the list of requests/actions 
        """

        ## (1) check input data 
        assert isinstance ( tree , ROOT.TTree ) , \
            "Invalid `tree` type: %s" % typename ( tree ) 

        ## (2) check the configurtaion of requests 
        requests = self.requests ( requests )

        ## (3)  more checks the requests
        for r in requests :
            if r.outvar in tree :
                logger.error ( 'Variable %s already in the ROOT.TTree, skip processing!' % r.outvar )
                return tree
            elif isinstance ( r.ntrk, string_types ) : pass
            elif self.good_for_sampling ( r.ntrk ) and self.nTrk_name and self.nTrk_name in tree :
                logger.error ( 'Variable %s already in the ROOT.TTree, skip processing!' % self.nTrk_name )
                return                                
        
        ## (3) chain processing ?
        if isinstance ( tree , ROOT.TChain ) and 1 < tree.nFiles :
            return self.__run_chain ( tree     ,
                                      requests = requests ,
                                      progress = progress ,
                                      report   = report   ,
                                      silent   = silent   , 
                                      parallel = parallel , **kwargs )

        
        ## (4) single tree processing
        
        the_file = tree.files [ 0 ]
        the_path = tree.full_path

        results  = {}
        
        ## number of entries 
        N = len ( tree )

        ## sampled #tracks 
        sampled_ntrk = None  
        
        for request in progress_bar ( requests , silent = not progress , description = 'Requests:' ) : 

            if not progress and not silent :
                logger.info ( 'Processing request: variable/sample/dataset : %s/%s %s' % ( request.variable ,
                                                                                           request.sample   ,
                                                                                           request.dataset  ) )
            ## unpack                
            invar , pt , eta, ntrk = request.invar , request.pt , request.eta , request.ntrk
            
            ## sample ntrk if requested
            if sampled_ntrk is None and not isinstance ( ntrk , string_types ) :
                sampled_ntrk = self.sample_ntrk ( ntrk , N = N ) 

            ## get the data 
            data = self.get_data ( tree , invar , pt , eta , ntrk = ntrk if sampled_ntrk is None else sampled_ntrk )
            
            ## run the actual pidgen machinery 
            pids , _ , _ = self ( data             ,
                                  request.sample   ,
                                  request.dataset  ,
                                  request.variable ) 
            
            ## collect the results 
            results [ request.outvar ] = pids 

        ## addd #ntrk is requested
        if not sampled_ntrk is None and self.nTrk_name :
            results [ self.nTrk_name ] = sampled_ntrk 
   
        chain = ROOT.TChain ( the_path )
        chain.Add ( the_file ) 
        return chain.add_new_buffer ( results , report = report , progress = progress ) 

    # =========================================================================
    ## Process a chain  (sequence of trees)
    #  @param chain     (INPUT/UPDATE)  input/update TTree/TChain 
    #  @param requests  (INPUT)         the list of requests/actions 
    def __run_chain  ( self             ,
                       chain            ,
                       requests         , * , 
                       progress = True  ,  
                       report   = True  ,
                       silent   = False ,
                       parallel = False , **kwargs ) :
        """ Process a chain  (sequence of trees)
        - chain     (INPUT/UPDATE)  input/update TTree/TChain 
        - requests  (INPUT)         the list of requests/actions 
        """
        
        ## (1) check chain/tree 
        assert isinstance ( chain , ROOT.TTree  ) , "Invalid `chain` type: %s" % typename ( chain )
        if not isinstance ( chain , ROOT.TChain ) or 2 > chain.nFiles  :
            return self.run ( chain    ,
                              requests = requests ,
                              silent   = silent   ,
                              progress = progress ,
                              report   = report   ,
                              parallel = False    , **kwargs ) 

        ## (2) check the configuration of requests 
        if isinstance ( requests , Correct ) : requests = requests,
        assert isinstance ( requests , sequence_types ) and \
            all ( isinstance ( r , Correct ) for r in requests ) , \
            "Invalid `requests` type %s" % typename ( requests )
        
        # ========================================================================================
        ## list of existing branches/leaves 
        branches = ( set ( chain.branches() ) | set ( chain.leaves() ) ) if report else set() 

        ## chain full name 
        cname = chain.fullpath 
        ## files to be processed 
        files = chain.files 

        ## parallel processing 
        if parallel and files :
            
            ## create the task for paralell processing
            task  = PidTask ( self, requests )
            
            from ostap.trees.utils import Chain
            ch    = Chain      ( chain )    
            trees = ch.split   ( chunk_size = -1 , max_files = 1  )
    
            ## Manager
            from   ostap.parallel.parallel import WorkManager
            wmgr   = WorkManager ( silent = silent , progress = progress , **kwargs )
            wmgr.process ( task , trees )

        else :

            local_progress = progress and ( 5 <= len ( files ) ) 
            down_progress  = progress and ( 5 >  len ( files ) ) 
                        
            ## sequential processing of input trees
            from ostap.utils.progress_bar import progress_bar
            for fname in progress_bar ( files , silent = not local_progress , description = "Files:" ) :
                if not local_progress and not silent : logger.info ( 'Processing file: %s' % fname )
                tree = ROOT.TChain ( cname  )
                tree.Add ( fname )
                self.run ( tree                     ,
                           requests = requests      ,                 
                           progress = down_progress , 
                           report   = False         ,
                           silent   = silent or local_progress  , 
                           parallel = False         ,  **kwargs ) 

        # =====================================================================
        ## reconstruct the resulting chain 
        chain = ROOT.TChain ( cname )
        for fname in files : chain.Add ( fname )
        
        if report :        
            new_branches = sorted ( ( set ( chain.branches () ) | set ( chain.leaves () ) ) - branches )
            if new_branches :
                n = len ( new_branches )
                if 1 >= n : title = "Added %s branch to TChain(%s)"   % ( n , cname ) 
                else      : title = "Added %s branches to TChain(%s)" % ( n , cname )
                table = chain.table ( new_branches , title = title , prefix = '# ' )
                logger.info ( '%s:\n%s' % ( title , table ) ) 
                chain = ROOT.TChain ( cname )
                for fname in files : chain.Add ( fname )
                
        return chain 
    
# =============================================================================
## @class PidTask
#  Simple Task for the parallel processing of PidGen
class PidTask(Task) :
    """ Simple Task for the parallel processing of PidGen
    """
    def __init__ ( self , pidobj , requests ) :
        
        self.__pidobj   = pidobj 
        self.__requests = pidobj.requests ( requests ) 
    
    ## local initialization (executed once in parent process)
    def initialize_local   ( self ) : pass 
        
    # =============================================================
    ## the actual processing
    def process ( self , jobid , item ) :
        """ The actual processing
        """
        from ostap.logger.utils import logWarning
        with logWarning() :
            import ROOT
            from   ostap.math.base      import all_entries 
            from   ostap.parallel.utils import random_random 
            
        ## 
        random_random ( jobid , *self.__requests )
        
        chain = item.chain 
        first = item.first
        last  = item.last
        
        assert all_entries ( chain , first , last ) , \
            "Only the whole TTree/TChain can be processed! "
        
        self.__pidobj.run ( chain            ,
                            self.__requests  ,  
                            progress = False , 
                            report   = False ,
                            silent   = True  , 
                            parallel = False )
        
    ## merge results 
    def merge_results ( self , result , jobid = -1 ) : pass    
    ## get the results 
    def results       ( self ) : pass 

    
# =============================================================================
if '__main__' == __name__ :
    
    from ostap.utils.docme import docme
    docme ( __name__ , logger = logger )
    
# =============================================================================
##                                                                      The END 
# =============================================================================
