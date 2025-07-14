#!/usr/bin/env python
# -*- coding: utf-8 -*-
## @file  pidgen.py
#  A tiny wrapper for Anton Poluektov's pidgen2/pidcorr machinery
#  @see https://pypi.org/project/pidgen2
#  It allows to add the new resampled variables directly into the input TTree/TChain
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
#  @endcode
#
#  For input data-chais one can create TChani manualy (as shown above) or
#  using <code>ostap.trees.data_utils.Data</code> utility`
#
#  IMPORTANT: It updates the input data, therefore
#   - One can *not* process the input dat via the READ-ONLY protocols
#   - CERN /eos is not reliable storage for modifications of dataL
#     ROOT tends to segfault when data in /eos/ are modified 
# 
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
  using ostap.trees.data_utils.Data utility 
                                                                             
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
)
# =============================================================================
from   collections            import namedtuple
from   ostap.core.ostap_types import sequence_types
from   ostap.parallel.task    import Task
import ostap.utils.cleanup    as     CU 
import ostap.trees.trees
import ROOT 
# =============================================================================
## Logging 
# =============================================================================
from   ostap.logger.logger import getLogger
logger = getLogger ( 'ostap.tools.pidcorr' )
# =============================================================================
## helper class to define input data 
Correct = namedtuple ( 'Correct'  , ( 'invar'    ,   ## input variable to be corrected     
                                      'outvar'   ,   ## corrected (output) variable 
                                      'sample'   ,   ## Calibration sample,  e.g. 'pi_Dstatt2Dpi'
                                      'dataset'  ,   ## Calibration dataset  e.g. 'MagDown_2012'
                                      'variable' ,   ## Calibration variable e.g.  'MC!2TuneV1_ProbNNpi'
                                      'pt'       ,   ## how to get pt in MeV 
                                      'eta'      ,   ## how to get eta 
                                      'ntrk'     ) ) ## how to get number of tracks ) 
# =============================================================================
## @class PidCorr
#  A tiny wrapper for Anton Poluektov's pidgen2 machinery
#  @see https://pypi.org/project/pidgen2
#  - It allows to add the new resampled variable directly into the input TTree/TChain#
#  @code
#  The usage if fairly transparent:
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
#  @endcode
# 
#  IMPORTANT: It updates the input data, therefore
#   - One can *not* process the input dat via the READ-ONLY protocols
#   - CERN /eos is not reliable storage for modifications of dataL
#     ROOT tends to segfault when data in /eos/ are modified 
#
#  @author Vanya BELYAEV Ivan.Belyaev@cern.ch
#  @date   2025-07-12
class PidCorr(object) :
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
    using ostap.trees.data_utils.Data utility 
    
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
    
    IMPORTANT: It updates the input data, therefore
    - One can *not* process the input dat via the READ-ONLY protocols
    - CERN /eos is not reliable storage for modifications of dataL
     ROOT tends to segfault when data in /eos/ are modified 

    """
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
    def __init__ ( self , simversion , **kwargs ) :
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
        self.__kwargs = {
            'simversion'  : simversion , 
            'library'     : 'ak'       ,
            'plot'        : False      ,
            'eta_from_p'  : False      ,
            'usecache'    : True       , 
            'verbose'     : -1         , 
        }
        self.__kwargs.update ( kwargs )
        verbose = self.__kwargs.get ( 'verbose' , 0 ) 
        if not 'local_storage' in self.__kwargs :
            dir1 = CU.CleanUp.tempdir ( prefix = 'ostap-PidCorr-templates-'    )
            if 0 < verbose : logger.info ( 'Local    template storage : %s' % dir1 ) 
            self.__kwargs [ 'local_storage'    ] = dir1            
        if not 'local_mc_storage' in self.__kwargs :
            dir2 = CU.CleanUp.tempdir ( prefix = 'ostap-PidCorr-mc_templates-' )
            if 0 < verbose : logger.info ( 'Local mc_template storage : %s' % dir2 ) 
            self.__kwargs [ 'local_mc_storage' ] = dir2
            
    # ==========================================================================
    ## Check existence/validity of sample/dataset combination 
    @staticmethod 
    def check ( requests ) :
        """ Check existence/validity  of sample/dataset combination         
        """
        if isinstance ( requests , Correct ) : requests = requests,
        assert isinstance ( requests , sequence_types ) and \
            all ( isinstance ( r , Correct ) for r in requests ) , \
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
    ## The minimal action: process a single  input file
    #  @param tree      (INPUT/UPDATE)  input/updat TTree 
    #  @param requests  (INPUT)         the list of requests/actions 
    def __process_tree ( self      ,
                         tree      , 
                         requests  ,
                         report    = False ,
                         progress  = False ,
                         parallel  = False , **kwargs ) :
        """ The minimal action: process a single  input file
        - file_tree (INPUT/UPDATE)  input/updat TTree 
        - requests  (INPUT)         the list of requests/actions 
        """
        assert isinstance ( tree , ROOT.TTree ) , \
            "Invalid `tree` type: %s" % typename ( tree ) 
        
        if isinstance ( tree , ROOT.TChain ) and 1 < tree.nFiles() :
            return self._process_chain ( self     ,
                                         tree     ,
                                         requests ,
                                         report   = report   ,
                                         progress = progress ,
                                         parallel = parallel , *kwargs ) 

        the_file = tree.files()[0]
        the_path = tree.full_path
        
        input    = f'{the_file}:{the_path}'

        results  = {}
        
        for request in requests :

            invar , pt , eta, ntrk = request.invar , request.pt , request.eta , request.ntrk
            
            branches = f'{invar}:{pt}:{eta}:{ntrk}'
            
            ## the output file
            output   = CU.CleanUp.tempfile ( prefix = 'ostap-PidCorr-' , suffix = '.root' )
            
            ## (1) run the pidcorr machinery 
            from pidgen2.correct import correct
            correct  ( input    = input    ,
                       sample   = request.sample   ,
                       dataset  = request.dataset  ,
                       variable = request.variable , 
                       branches = branches         , 
                       output   = output           , 
                       outtree  = the_path         ,
                       pidcorr  = request.outvar   , 
                       friend   = True             ,
                       **self.__kwargs             )
            
            ## (2) Access the produced output friend tree
            chfr = ROOT.TChain ( the_path )
            chfr.Add ( output )

            ## x
            obranches = chfr.branches ( '%s.*' % request.outvar  )
            data, w = chfr.slice ( obranches , structured = True )

            for n in data.dtype.names :
                results [ n ] = data [ n ]

        
        chain = ROOT.TChain ( the_path )
        chain.Add ( the_file ) 
        return chain.add_new_buffer ( results , report = report , progress = progress ) 
            
    # =========================================================================
    ## Process a chain  (sequence of trees)
    #  @param chain     (INPUT/UPDATE)  input/updat TTree 
    #  @param requests  (INPUT)         the list of requests/actions 
    def __process_chain  ( self             ,
                           chain            ,
                           requests         , 
                           silent   = True  ,
                           progress = True  ,  
                           report   = True  ,
                           parallel = False , **kwargs ) :
        """ Process a chain  (sequence of trees)
        - chain     (INPUT/UPDATE)  input/updat TTree 
        - requests  (INPUT)         the list of requests/actions 
        """

        ## (1) check chain/tree 
        assert isinstance ( chain , ROOT.TTree  ) , "Invalid `chain` type: %s" % typename ( chain )
        if not isinstance ( chain , ROOT.TChain ) or 2 > chain.nFiles() :
            return self.__process_tree ( chain    ,
                                         requests ,
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
        cname = chain.fullPpath 
        ## files to be processed 
        files = chain.files()

        ## parallel processing 
        if parallel and files :
            
            ## create the task for paralell processing
            task = PidCorrTask ( requests , **self.__kwargs )
            from ostap.trees.utils import Chain
            ch    = Chain      ( chain )    
            trees = ch.split   ( chunk_size = -1 , max_files = 1  )
    
            ## Manager
            from   ostap.parallel.parallel import WorkManager
            wmgr   = WorkManager ( silent = silent , progress = progress or not silent , **kwargs )
            wmgr.process ( task , trees )

        else :

            chain_progress = progress and ( 5 <= len ( files ) ) 
            tree_progress  = progress and ( 5 >  len ( files ) ) 
                        
            ## sequential processing iof input trees
            from ostap.utils.progress_bar import progress_bar
            for fname in progresS_bar ( files , silent = not chain_progress , description = "TTrees:") :
                tree = ROOT.TChain ( chain.cname  )
                tree.Add ( fname )
                self.__process_tree ( tree                     ,
                                      requests ,                 
                                      progress = tree_progress , 
                                      report   = False         ,
                                      parallel = False         , **kwargs ) 

        # =====================================================================
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
    
    # ========================================================================================
    ## The major method for processing of  inptu data in a form of (chain, [requests] ) pairs
    #  @param the_requests    (INPUT)  the sequence (chain,[requests]) pairs    
    def process ( self                 ,
                  the_requests         ,
                  silent       = False , 
                  progress     = False ,
                  report       = False , 
                  parallel     = False , **kwargs ) :
        """ The major method for processing of  inptu data in a form of (chain, [requests] ) pairs
        - the_requests    (INPUT)  the sequence (chain,[requests]) pairs    
        """
        ## (1) initial loop over the entries 
        for tree , requests in the_requests  :
            assert isinstance ( tree , ROOT.TTree ) , \
                "Invalid type for `tree` %s" % typename ( tree )
            PidCorr.check ( requests ) 
            outvars = set() 
            for r in requests :
                if not r.invar   in tree :
                    logger.error ( 'Variable %s not in the TTree, skip processing!'     % r.invar  )
                    return                                                           ## RETURN
                elif r.outvar in tree :
                    logger.error ( 'Variable %s already in the TTree, skip processing!' % r.outvar )
                    return                                                           ## RETURN 
                elif r.outvar in outvars :
                    logger.error ( 'Variable %s defined twice!' % r.outvar )
                    return                                                           ## RETURN 
                outvars.add ( r.outvar ) 
            for f in tree.files () :
                if '/eos/' in f : logger.warning ( 'EOS if not  areliable storage for safe modification of data!' )
                
        NR = len ( the_requests ) 
        global_progress = progress and 10 <= NR 
        chain_progress  = progress and 10 >  NR 
        
        silent          = progress or silent 
        
        from ostap.utils.progress_bar import progress_bar
        for tree , requests in progress_bar ( the_requests , silent = not global_progress , description = "Chains:" ) :
            self.__process_chain ( tree     ,
                                   requests ,
                                   silent   = silent         , 
                                   progress = chain_progress ,
                                   report   = report         , 
                                   parallel = parallel       , **kwargs ) 
        

# =============================================================================
## @class PidCorrTask
#  Simple Task for the parallel processing of PidCorr
class PidCorrTask(Task) :
    """ Simple Task for the parallel processing of PidCorr
    """
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
    
    # =========================================================================
    ## the actual processing
    def process ( self , jobid , item ) :
        """ The actual processing
        """
        from ostap.logger.utils import logWarning
        with logWarning() :
            import ROOT
            from   ostap.math.base       import all_entries 
            from   ostap.paralllel.utils import random_random 
            from   pidcorr               import PidCorr
            
        ## 
        random_random ( jobid , *self.__requests )
        
        chain = item.chain 
        first = item.first
        last  = item.last
        assert all_entries ( chain , first , last ) , \
            "Only the whole TTree/TChain can be processed! "
        
        ## create the object
        pcorr  = PidCorr  ( **self.__kwargs ) 
        result = pcorr.process ( tree             ,
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

