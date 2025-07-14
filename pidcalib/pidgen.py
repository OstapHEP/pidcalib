#!/usr/bin/env python
# -*- coding: utf-8 -*-
## @file  pidgen.py
#  A tiny wrapper for Anton Poluektov's pidgen2 machinery
#  @see https://pypi.org/project/pidgen2
#  - It allows to add the new corrected variables directly into the input TTree/TChain
#
# ******************************************************************************
# *                                                                            *
# *    ooooooooo.    o8o        .o8    .oooooo.                                * 
# *    `888   `Y88.  `"'       "888   d8P'  `Y8b                               * 
# *     888   .d88' oooo   .oooo888  888            .ooooo.  ooo. .oo.         *
# *     888ooo88P'  `888  d88' `888  888           d88' `88b `888P"Y88b        *
# *     888          888  888   888  888     ooooo 888ooo888  888   888        *
# *     888          888  888   888  `88.    .88'  888    .o  888   888        * 
# *    o888o        o888o `Y8bod88P"  `Y8bood8P'   `Y8bod8P' o888o o888o       *
# *                                                                            *
# ******************************************************************************
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
#
#  pgen = RunPidGen ( ...  )
#  pgen.process     ( requests , progress = True , .... )
#
#  @encode
#
#  `PidGen` feeds the `pidgen2.resampler.create_resampler` function with following arguments 
#        - `sample`
#        - `dataset`
#        - `variable`
# 
#  All other arguments can be provided via `PidCorr` constructor
#
#  IMPORTANT:It updates the input data, therefore
#   - One can *not* process the input dat via the READ-ONLY protocols
#   - CERN /eos is not reliable storage for modifications of dataL
#     ROOT tends to segfault when data in /eos/ are modified 
# 
#  @author Vanya BELYAEV Ivan.Belyaev@cern.ch
#  @date   2025-07-12
# =============================================================================
"""  A tiny wrapper for Anton Poluektov's pidgen2 machinery
- see https://pypi.org/project/pidgen2
- It allows to add the new corrected variables directly into the input TTree/TChain

  ***************************************************************************
  *                                                                         *
  *    ooooooooo.    o8o        .o8    .oooooo.                             * 
  *    `888   `Y88.  `"'       "888   d8P'  `Y8b                            * 
  *     888   .d88' oooo   .oooo888  888            .ooooo.  ooo. .oo.      *
  *     888ooo88P'  `888  d88' `888  888           d88' `88b `888P"Y88b     *
  *     888          888  888   888  888     ooooo 888ooo888  888   888     *
  *     888          888  888   888  `88.    .88'  888    .o  888   888     * 
  *    o888o        o888o `Y8bod88P"  `Y8bood8P'   `Y8bod8P' o888o o888o    *
  *                                                                         *
  ***************************************************************************

Usage is fairly trivial:

  ## input MC data to be resampled 

  >>> data_2016u = ROOT.TChain ( ... )
  >>> data_2016u = ROOT.TChain ( ... )

  >>> from pidcalib.pidgen import PidGen, ReSample

  >>> requests = [
  ##                            out-var     sample           dataset          cabration variable        pt in MeV        pseudorapidity  #tracks
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
 
  All other arguments can be provided via `PidCorr` constructor

  IMPORTANT: It updates the input data, therefore
   - One can *not* process the input dat via the READ-ONLY protocols
   - CERN /eos is not reliable storage for modifications of dataL
     ROOT tends to segfault when data in /eos/ are modified 

"""
# =============================================================================
__version__ = "$Revision$"
__author__  = "Vanya BELYAEV Ivan.Belyaev@cern.ch"
__date__    = "2025-07-12"
__all__     = (
    'ReSample'  , ## helper class to descrribe the elementary
    'PidGen'    , ## run pidgen2 machinery for the single chain/tree 
)
# =============================================================================
from   collections            import namedtuple
from   ostap.core.ostap_types import sequence_types 
from   ostap.utils.basic      import typename 
from   ostap.parallel.task    import Task
import ostap.trees.trees
import ROOT, numpy 
# =============================================================================
## Logging 
# =============================================================================
from   ostap.logger.logger import getLogger
logger = getLogger ( 'ostap.tools.pidgen' )
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
# ===============================================================================
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
#  Number of tracks can be specified in various ways: 
#  - name        e.g. "nTrack"        - usin the initial #track varibe from MC 
#  - expression  e.g. "nTrack*1.20"   - scale them by 20%
#  - 1D-histogram of #ntracks          #ntrk will be sampled fom the histogram
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
class PidGen(object):
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
    
    Number of tracks can be specified in various ways: 

    - name        e.g. "nTrack"        - usin the initial #track varibe from MC 
    - expression  e.g. "nTrack*1.20"   - scale them by 20%
    - 1D-histogram of #ntracks         - #ntrk will be sampled fom the histogram

   `PidGen` feeds the `pidgen2.resampler.create_resampler` function with following arguments 
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
    ## Create resampler
    #  @see pidgen2.resampler
    #  @see pidgen2.resampler.create_resampler     
    def __init__ ( self , **kwargs ) :
        """
        "" Create resampler
        - see pidgen2.resampler
        - see pidgen2.resampler.create_resampler     
        """
        ## All these variables will be forwarded to to Anton's resampler 
        self.__kwargs   = kwargs 
        ## 

    # ==========================================================================
    ## Check existence/validity of sample/dataset combination 
    @staticmethod 
    def check ( requests ) :
        """ Check existence/validity  of sample/dataset combination         
        """
        if isinstance ( requests , ReSample ) : requests = requests,
        assert isinstance ( requests , sequence_types ) and \
            all ( isinstance ( r , ReSample ) for r in requests ) , \
            "Invalid `requests` type %s" % typename ( requests )
        
        ## 
        from pidgen2.resampling import get_samples 
        samples = get_samples()
        for r in requests :
            the_sample = samples.get ( r.sample , None )
            assert the_sample and r.dataset in the_sample , \
                "Invalid/non-existing sample/dataset: %s/%s" % ( r.sample , r.dataset )
        return True

    # =========================================================================
    ## Run PidGen machinery for several variables/samples/datasets/trees
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
    def process ( self ,
                  the_requests     , 
                  progress = True  ,
                  report   = False ,
                  silent   = False ,
                  parallel = False , **kwargs ) : 
        """ Run PidGen machinery for several variables/samples/datasets/trees
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
        for tree , requests in the_requests  :
            assert isinstance ( tree , ROOT.TTree ) , \
                "Invalid type for `tree` %s" % typename ( tree )
            if isinstance ( requests , ReSample ) : requests = requests,
            assert isinstance ( requests , sequence_types ) and \
                all ( isinstance ( r , ReSample ) for r in requests ) , \
                "Invalid `requests` type %s" % typename ( requests )
            PidGen.check ( requests ) 
            for r in requests :
                if r.outvar in tree :
                    logger.error ( 'Variable %s already in the ROOT.TTree, skip processing!' % r.outvar )
                    return
            for f in tree.files () :
                if '/eos/' in f : logger.warning ( 'EOS if not a reliable storage for safe modification of data!' )
                

        ## (2) start pidgen processing
        for tree , requests in the_requests :
            self.run ( tree                ,
                       requests            ,
                       report   = report   ,
                       progress = progress ,
                       parallel = parallel , **kwargs )
            
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
        from   ostap.utils.basic import typename
        
        assert isinstance  ( data , numpy.ndarray ) , "Invalid `data` type %s" % ( typename ( data ) )
        assert 2 == len ( data.shape )              , "Invalid data shape %s" % str ( data.shape )
        assert 3 == data.shape[1]                   , "Invalid data shape %s" % str ( data.shape )

        ## use pidgen2 machinery!!! 
        from pidgen2.resampler import create_resampler
        resampler = create_resampler (
            sample   = sample   ,
            dataset  = dataset  ,
            variable = variable ,            
            **self.__kwargs )
        
        return resampler ( data ) 
    
    # =========================================================================
    ## Internal function to run pidgen machinery over TChain with many files
    def __run_chain ( self             ,
                      chain            , ## input TTree/TChain
                      requests         , ## requests  
                      report   = True  , ## make a report ? 
                      progress = True  , ## show progress
                      parallel = False , ## use the parallel processing ?  
                      silent   = True  , 
                      **kwargs         ) : 
        """ Internal function to run pidgen machinery over TChai with many files 
        """
        ## (1) treat the input chain/tree 
        assert isinstance ( chain  , ROOT.TTree ) , "Invalid type of `chain`!"        
        ## simple tree ?
        if not isinstance ( chain  , ROOT.TChain ) or 2 > chain .nFiles() :
            return self.run ( chain    ,
                              requests , 
                              report   = report   ,
                              progress = progress , **kwargs )

        ## (2) check the configuration of requests 
        if isinstance ( requests , ReSample ) : requests = requests,
        assert isinstance ( requests , sequence_types ) and \
            all ( isinstance ( r , ReSample ) for r in requests ) , \
            "Invalid `requests` type %s" % typename ( requests )

        ## check request
        for r in requests :
            if r.outvar in chain :
                logger.error ( 'Variable %s already in the TTree, skip processing!' % r.outvar )
                return 

        # ========================================================================================
        ## list of existing branches/leaves 
        branches = ( set ( chain.branches() ) | set ( chain.leaves() ) ) if report else set() 
        
        ## files to be processed 
        files = chain.files()

        ## parallel processing? 
        if parallel and files :
            
            ## create the talk for paralell processing
            task = PidGenTask ( requests , **self.__kwargs )
            from ostap.trees.utils import Chain
            ch    = Chain    ( chain )    
            trees = ch.split ( chunk_size = -1 , max_files = 1  )
    
            ## Manager
            from   ostap.parallel.parallel import WorkManager
            wmgr   = WorkManager ( silent = silent , progress = progress or not silent , **kwargs )
            wmgr.process ( task , trees )

        else :
            ## sequential procesinsg here :
    
            chain_progress = progress and ( 5 <= len ( files ) ) 
            tree_progress  = progress and ( 5 >  len ( files ) ) 

            from ostap.utils.progress_bar import progress_bar 
            for fname in progress_bar ( files , silent = not chain_progress , description = 'Files:' ) :
                tree = ROOT.TChain ( cname )
                tree.Add ( fname ) 
                ## treat the tree 
                self.run ( tree     ,
                           requests , 
                           report   = False         ,
                           progress = tree_progress , **kwargs ) 

        ## reconstruct the resulting chain 
        chain = ROOT.TChain ( cname )
        for fname in files : chain.Add ( fname )
        
        if report :        
            new_branches = sorted ( ( set ( chain.branches () ) | set ( chain.leaves () ) ) - branches )
            if new_branches :
                n = len ( new_branches )
                if 1 >= n : title = 'Added %s branch to TChain'   % n 
                else      : title = 'Added %s branches to TChain' % n 
                table = chain.table ( new_branches , title = title , prefix = '# ' )
                logger.info ( '%s:\n%s' % ( title , table ) ) 
                chain = ROOT.TChain ( cname )
                for fname in files : chain.Add ( fname )
                
        return chain 
                    
    # =========================================================================
    ## Run pidgen machinery for several request for single tree/chain 
    #  @param tree    (UPDAT) the input/update TTree/TChain
    #  @param varname (INPUT) the varable to be added into TTree/TChain
    #  @param pt      (INPUT) the name/expression to get "pt in GeV"
    #  @param eta     (INPUT) the name/expression to get "eta"
    #  @param ntrk    (INPUT) a way to get @ntrack values
    #
    #  Number of tracks can be specified 
    #  - by name        e.g. "nTrack"        - usin the initial #track varibe from MC 
    #  - by expression  e.g. "nTrack*1.20"   - scale them by 20%
    #  - by the histogram of #ntarcks distribution, in this case #ntrack wil lbe sampled fom the histogram         
    def run ( self             ,
              tree             , ## input TTree/TChain
              requests         , ## requests to process 
              report   = True  , ## make a report ? 
              progress = True  , ## show progress ?
              parallel = False , 
              **kwargs         ) :
        """ Run pidgen machinery for several request for single tree/chain 
        """
        ## (1) treet input data 
        assert isinstance ( tree , ROOT.TTree ) , "Invalid type of `tree`!"
        if isinstance ( tree , ROOT.TChain ) and 1 < tree.nFiles () :
            return self.__run_chain ( tree                ,
                                      requests            , 
                                      report   = report   ,
                                      progress = progress ,
                                      parallel = parallel ,
                                      **kwargs )

        ## (2) check the configuration of requests 
        if isinstance ( requests , ReSample ) : requests = requests,
        assert isinstance ( requests , sequence_types ) and \
            all ( isinstance ( r , ReSample ) for r in requests ) , \
            "Invalid `requests` type %s" % typename ( requests )

        ## check request
        for r in requests :
            if r.outvar in tree :
                logger.error ( 'Variable %s already in the TTree, skip processing!' % r.outvar )
                return 

        ## number of entries 
        N = len ( tree )

        ## sampled #tracks 
        sampled_ntrk = None
        
        ## some tree utilities 
        from ostap.trees.cuts  import vars_and_cuts, expression_types 

        results = {}

        ## loop over input requests 
        for request in requests :

            ## recipes to access pt, eta and ntrk varibales 
            pt , eta , ntrk = request.pt , request.eta , request.ntrk

            ## special case: if ntrk is a histogram, sample from this histogram!
            if self.__good_for_sampling ( ntrk ) : 
                
                if sampled_ntrk is None :                    
                    ## generate ntrk from  the supplied histogram 
                    sampled_ntrk = numpy.asarray ( [ int ( v ) for v in ntrk.shoot ( N , lambda s : 0 < s ) ] , dtype = numpy.float64 ) 
                    
                    var_lst , _ , _ = vars_and_cuts ( [ pt , eta ]  , '' )
                assert 2 == len ( var_lst ) , "Invalid setting of pt/eta: %s" % [ pt , eta ]  
                
                data , _ = tree.slice ( var_lst , '' , structured = False , transpose = True )
                data     = numpy.column_stack ( [ data , sampled_ntrk ] ) 
                
            elif isinstance ( ntrk , expression_types ) :
                
                var_lst , _ , _ = vars_and_cuts ( [ pt , eta , ntrk ]  , '' )
                assert 3 == len ( var_lst ) , "Invalid setting of pt/eta/ntrk: %s" % [ pt , eta , ntrk ] 
                ## get data from the tree 
                data , _ = tree.slice ( var_lst , '' , structured = False , transpose = True )
            
            else :

                raise TypeError ( "Unknown/Invalid  type for ntrk: %s" % typename ( ntr ) )
    
            ## run the actual pidgen machinery 
            pids , _ = self ( data             ,
                              request.sample   ,
                              request.dataset  ,
                              request.variable ) 
            
            ## collect the results 
            results [ request.outvar ] = pids 

        ## add result to TTree:
        return tree.add_new_buffer ( results , report = report , progress = progress )

    # =========================================================================
    ## Is the histogram good enough for #ntrk sampling ? 
    def __good_for_sampling ( self , histo ) :
        """ Is the histigram good for #ntrk sampling ? 
        """
        import ostap.histos.histos
        
        if not isinstance ( histo , ROOT.TH1 ) : return False
        if not 1 != histo.GetDimension()       : return False 
        
        xmin, xmax = histo.xminmax()
        if xmax < 100                          : return False  ## upper limit cannot be smaller than 100! 

        last_bin = len ( histo ) 
        zero_bin = 1 if 0 <= xmim else nmax ( 1 , histo.FindBin ( 0 ) )
        if zero_bin < last_bin                 : return False
        #
        return  0 < histo.Integral ( zero_bin , last_bin )
        
# =============================================================================
## @class PidGenTask
#  simple Task for the parallel processing of PidGen
class PidGenTask(Task) :
    def __init ( self      ,
                 requests  ,
                 **kwargs  ) :
        
        if isinstance ( requests , ReSample ) : requests = requests,
        assert isinstance ( requests , sequence_types ) and \
            all ( isinstance ( r , ReSample ) for r in requests ) , \
            "Invalid `requests` type %s" % typename ( requests )
        
        self.__requests = requests 
        self.__kwargs   = kwargs

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
            from   ostap.math.base       import all_entries 
            from   ostap.paralllel.utils import random_random 
            from   pidgen                import PidGen
            
        ## 
        random_random ( jobid , *self.__requests )
        
        chain = item.chain 
        first = item.first
        last  = item.last
        assert all_entries ( chain , first , last ) , \
            "Only the whole TTree/TChain can be processed! "
        
        ## create the object
        pgep   = PidGen ( **self.__kwargs ) 
        result = pgen.run ( tree             ,
                            self.requests    ,                           
                            report   = False ,
                            progress = False , 
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
