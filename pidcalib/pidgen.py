#!/usr/bin/env python
# -*- coding: utf-8 -*-
# =============================================================================
## @file  pidgen.py
#  Machinery to resamle PD responsce (PIDGen) and to add it into the initial TTree/Tchain 
#  @author Vanya BELYAEV Ivan.Belyaev@cern.ch
#  @date   2023-06-05
#  @attention Run-time dependency on Urania is required!
# =============================================================================
""" Machinery to resamle PD responsce (PIDGen) and to add it into the initial TTree/Tchain 
- attention Run-time dependency on Urania is required!
"""
# =============================================================================
__version__ = "$Revision$"
__author__  = "Vanya BELYAEV Ivan.Belyaev@cern.ch"
__date__    = "2023-06-05"
__all__     = (
    'PidGen'        , ## the main configurtaoin class 
    'runPidGen'     , ## function to run pid-gen machinery
    'run_pid_gen'   , ## function to run pid-gen machinery
    )
# =============================================================================
from   ostap.core.pyrouts       import hID, std, SE
from   ostap.math.reduce        import root_factory
from   ostap.parallel.parallel import Task, WorkManager
from   ostap.utils.progress_bar import progress_bar 
import ostap.io.root_file 
import ostap.trees.trees
# =============================================================================
import Meerkat ##  <--------------- ATTENTION!!!! 
import ROOT, os
# =============================================================================
from ostap.logger.logger import getLogger
logger = getLogger('pidcalib.pidgen')
# =============================================================================
## @class PidGen
#  Helper class for resampling PID variables
#  - It allows to add the new resmapled varibales directly into the input TTree/TChain
#  @author Vanya BELYAEV Ivan.Belyaev@cern.ch
#  @date   2023-06-05
class PidGen(object):
    """ Helper class for resampling of PID variables 
    - It allows to add the new resmapled varibales directly into the input TTree/TChain
    """
    def __init__ ( self                     ,
                   pt_log_var               , ## log(pt/MeV) 
                   eta_var                  , ## pseudorapidity 
                   ntrk_log_var             , ## log(#ntracks)                   
                   config  = 'pi_V3ProbNNpi',
                   dataset = 'MagUp_2011'   ,
                   variant = 'default'      ,
                   silent  = False          ) :
        
        self.__silent = True if silent else False 
        
        
        if 'log' in pt_log_var : self.__pt_log_var   = pt_log_var
        else :
            self.__pt_log_var   = 'log(%s)' % ( pt_log_var.strip() )
            logger.attention ( "Add 'log' to PT-variable: '%s'" % self.pt_log_var)
            
        self.__eta_var      = eta_var

        if 'log' in ntrk_log_var : self.__ntrk_log_var = ntrk_log_var
        else :
            self.__ntrk_log_var   = 'log(%s)' % ( ntrk_log_var.strip() )
            logger.attention ( "Add 'log' to #ntr-variable: '%s'" % self.ntr_log_var  )
            
        self.__config       = config
        self.__dataset      = dataset 
        
        if   '2011' in dataset : self.__run = 1
        elif '2012' in dataset : self.__run = 1
        elif '2015' in dataset : self.__run = 2
        elif '2016' in dataset : self.__run = 2
        elif '2017' in dataset : self.__run = 2
        elif '2018' in dataset : self.__run = 2
        else :
            raise TypeError ( 'Invalid dataset %s' % datatset )

        ## 
        if   1 == self.__run :
            import PIDGenExpert.Run1.Config as ConfigRun
            configs  = ConfigRun.configs 
        elif 2 == self.__run :
            import PIDGenExpert.Run2.Config as ConfigRun
            configs  = ConfigRun.configs() 
        
        self.__config_run = ConfigRun
        self.__variant    = 'distrib' if 'default' == variant else variant 

        
        if 2 == self.__run and 'gamma' in configs [config] :
            gamma = configs[config]['gamma']
            if gamma < 0:
                self.__transform_forward  = "(1.-(1.-x)**%f)" % abs ( gamma )
                self.__transform_backward = "(1.-(1.-x)**%f)" % ( 1. / abs ( gamma ) )
            else:
                self.__transform_forward  = "((x)**%f)"       % abs ( gamma )
                self.__transform_backward = "((x)**%f)"       % ( 1. / abs ( gamma ) )
        else : 
            self.__transform_forward  = configs[config]['transform_forward']
            self.__transform_backward = configs[config]['transform_backward']


        self.__calibfile  = os.path.join ( ConfigRun.eosrootdir , config , "%s_%s.root" % ( dataset, self.__variant ) )

        with ROOT.TFile.Open ( self.__calibfile , 'READ' ) as tf :
            if not self.__silent : tf.ls() 

        minpid = None ## ATTENTNION!!  
        pidmin = 0.
        pidmax = 1.
        if 'limits' in configs[config]:
            pidmin = configs[config]['limits'][0]
            pidmax = configs[config]['limits'][1]
        if minpid == None:
            minpid = pidmin
        else:
            minpid = float(minpid)
            if minpid < pidmin: minpid = pidmin

        # Calculate the minimum PID variable to generate (after transformation)
        x = pidmin
        self.__pidmin = eval ( self.__transform_forward )
        x = pidmax
        self.__pidmax = eval ( self.__transform_forward )
        x = minpid
        self.__minpid = eval ( self.__transform_forward )

        PS1D  = ROOT.OneDimPhaseSpace 
        PSCMB = ROOT.CombinedPhaseSpace 
        
        if self.__silent : ROOT.Logger.setLogLevel ( 1 )

        self.__pid_phsp       = PS1D  ( "PIDPhsp", self.__pidmin, self.__pidmax)        
        self.__mom_phsp       = PS1D  ( "MomPhsp", 5.5, 9.5 )        
        self.__eta_phsp       = PS1D  ( "EtaPhsp", 1.5, 5.5 )        
        self.__ntr_phsp       = PS1D  ( "NtrPhsp", 3.0, 6.5 )
        
        self.__pidmom_phsp    = PSCMB ( "PIDMomPhsp"    , self.__pid_phsp       , self.__mom_phsp)
        self.__pidmometa_phsp = PSCMB ( "PIDMomEtaPhsp" , self.__pidmom_phsp    , self.__eta_phsp)
        self.__phsp           = PSCMB ( "FullPhsp"      , self.__pidmometa_phsp , self.__ntr_phsp)
        
        ## finally: binned density 
        self.__kde = ROOT.BinnedDensity("KDEPDF", self.__phsp, self.__calibfile )

        ## the histogram 
        self.__histo = ROOT.TH1F ( hID() , "sampling histogram" , 100 , self.__minpid, self.__pidmax )

        self.__point    = std.vector('double')( 4 , 0.0 ) 

        ## various counters
        
        self.__total    = 0
        self.__pt_out   = 0
        self.__eta_out  = 0
        self.__ntrk_out = 0

        self.__log_pt   = SE ()
        self.__eta      = SE ()
        self.__log_ntrk = SE ()

        self.__hint     = SE () 

        self.__no_pid   = 0
        self.__low_stat = 0

        self.__newpid   = SE ()
        
        ROOT.Logger.setLogLevel ( 1 )

    ## reduce: serialize objects 
    def __reduce__ ( self ) :
        return root_factory, ( type ( self )     ,
                               self.pt_log_var   , 
                               self.eta_var      ,
                               self.ntrk_log_var ,
                               self.config       ,
                               self.dataset      , 
                               self.variant      ,
                               self.__silent     )
    
    ## the main method 
    def __call__ ( self , log_pt , eta , log_ntrk )  : 

        self.__total += 1
        
        if not 5.5 <= log_pt   <= 9.5 : self.__pt_out   += 1 
        if not 1.5 <= eta      <= 5.5 : self.__eta_out  += 1 
        if not 3.0 <= log_ntrk <= 6.5 : self.__ntrk_out += 1 

        
        if 4 != len ( self.__point ) : self.__point.resize ( 4 )
        
        self.__point [ 0 ] = 0.5 * ( self.__pidmin + self.__pidmax )
        self.__point [ 1 ] = log_pt 
        self.__point [ 2 ] = eta  
        self.__point [ 3 ] = log_ntrk 

        self.__histo.Reset()
        self.__kde.slice( self.__point , 0 , self.__histo )
        
        hint = self.__histo.Integral()
                
        if 0 < hint :
            x = self.__histo.GetRandom()
            if hint < 10: self.__low_stat +=1 
        else:
            x = self.__minpid + ( self.__pidmax - self.__minpid ) * ROOT.gRandom.Rndm()
            self.__no_pid += 1 
            
        newpid = eval ( self.__transform_backward )

        if not self.__silent : 
            self.__log_pt   += log_pt
            self.__eta      += eta 
            self.__log_ntrk += log_ntrk
            self.__newpid   += newpid 
            self.__hint     += hint
            
        return 1.0 * newpid 

    # ========================================================================
    ## list of branches used by PidGen 
    def branches ( self , tree ) :        
        return tree.the_variables ( self.pt_log_var , self.eta_var , self.ntrk_log_var )

    # ========================================================================
    ## List other available variants for given config/datataset
    #  @code
    #  pidgen   = PidGen ( ... )
    #  variants = pidgen.variants () 
    #  @endcode 
    def variants ( self ) :
        """List other available variants for given config/datataset
        >>> pidgen   = PidGen ( ... )
        >>> variants = pidgen.variants () 
        """
        vars = []
        items  = [ 'distrib' , 'default' ]
        items += [ 'stat_%s' % d for d in range ( 10 ) ]
        items += [ 'syst_%s' % d for d in range ( 10 ) ]
        
        from   ostap.core.core      import rootError 
        from   ostap.logger.logger  import logFatal
        
        for item in items :
            if self.__variant == item : continue 
            path = os.path.join ( self.__config_run.eosrootdir ,
                                  self.__config                ,
                                  "%s_%s.root" % ( self.__dataset, item ) )
            with logFatal() , rootError () : 
                rf = ROOT.TFile.Open ( path , 'READ' , exception = False )
                if rf and rf.IsOpen ( ) :
                    vars.append ( item )
                    rf.Close() 
                    
        return tuple ( vars )
        
    @property
    def pt_log_var ( self )  :
        """'pt_log_var' : log(pt/MeV)"""
        return self.__pt_log_var
    
    @property
    def eta_var ( self )  :
        """'eta_var' : pseudorapidity"""
        return self.__eta_var 
    @property
    def ntrk_log_var ( self )  :
        """'ntrk_log_var' : log(#nracks)"""
        return self.__ntrk_log_var 

    @property
    def config  ( self ) :
        """'config': configuration"""
        return self.__config
    
    @property
    def dataset ( self ) :
        """'dataset': dataset"""
        return self.__dataset
    
    @property
    def variant ( self ) :
        """'variant': variant"""
        return self.__variant
    
    @property
    def n_no_pid ( self ) :
        """'n_no_pid' : # of problematic calls with no PID information"""
        return self.__no_pid
    
    @property
    def n_low_stat ( self ) :
        """'n_lowstat' : # of low-statistics evalulations"""
        return self.__low_stat
 
    @property
    def n_pt_out ( self ) :
        """'n_pt_out' : # pt-variable is out of range """
        return self.__pt_out 
    
    @property
    def n_eta_out ( self ) :
        """'n_eta_out' : # eta-variable is out of range """
        return self.__eta_out 

    @property
    def n_ntrk_out ( self ) :
        """'n_ntrk_out' : # ntrk-variable is out of range """
        return self.__ntrk_out 

    @property
    def newpid ( self ) :
        """'newpid' : counter for newly generated PID response"""
        return self.__newpid 

    @property
    def cnt_log_pt ( self ) :
        """'cnt_log_pt' : counter for log(pt/MeV)-variable"""
        return self.__log_pt
    
    @property
    def cnt_eta ( self ) :
        """'cnt_eta' : counter for eta-variable"""
        return self.__eta
    
    @property
    def cnt_log_ntrk ( self ) :
        """'cnt_log_ntrk' : counter for log(#ntrk)-variable"""
        return self.__log_ntrk

    @property
    def cnt_hint ( self ) :
        """'cnt_hint' : counter for sampling histogram"""
        return self.__hint

    @property
    def total  ( self ) :
        """'total' : number of processed events"""
        return self.__total

    # =========================================================================
    ## make a summary table: configuration & statistics 
    def table  ( self , title = '' , prefix = '' ) :
        """Make a summary table:
        - configuration
        - statistics 
        """
        
        rows = [  ('' , 'value' ) ]

        row  = 'config'   , self.config
        rows.append ( row )
        
        row  = 'dataset/variant'      , '%s/%s' % ( self.dataset , self.variant )
        rows.append ( row )
        
        row  = 'log(pt/MeV) variable'  , self.pt_log_var
        rows.append ( row )
        
        row  = 'eta         variable'  , self.eta_var 
        rows.append ( row )
            
        row  = 'log(#ntrk)  variable'  , self.ntrk_log_var
        rows.append ( row )

        if self.total :
            row = '#processed ' , '%s' % self.total
            rows.append ( row )

            row = '#low-stats (<10)' , '%s' % self.n_low_stat
            rows.append ( row )
            
            row = '#no-PID'     , '%s' % self.n_no_pid  
            rows.append ( row )

            if self.__pt_out :
                row = '# pt out of range' , '%s' % self.n_pt_out 
                rows.append ( row ) 
            if self.__eta_out :
                row = '# eta out of range' , '%s' % self.n_eta_out 
                rows.append ( row ) 
            if self.__ntrk_out :
                row = '# trk out of range' , '%s' % self.n_ntrk_out 
                rows.append ( row )

            def fmt ( cnt , scale = 1 ) :
                f = 'mean+/-rms: %+.3f +/- %-.3f min/max: %+.3f/%-+.3f'                 
                return f % ( c.mean () / scale ,
                             c.rms  () / scale ,
                             c.min  () / scale ,
                             c.max  () / scale  )
            
            if not self.__silent :
                row = 'log(pt/MeV)'           , fmt ( self.cnt_log_pt   ) 
                rows.append ( row )                
                row = 'eta'                   , fmt ( self.cnt_eta      ) 
                rows.append ( row )
                row = 'log(#ntrk)'            , fmt ( self.cnt_log_ntrk ) 
                rows.append ( row )                
                row = 'histo-integral [10^3]' , fmt ( self.cnt_hint , 1000.0 )
                rows.append ( row ) 
                c   = self.newpid 
                row = 'new-PID'               , fmt ( self.newpid       )
                rows.append ( row ) 
                
        title = title if title else 'PidGen: configuration& statistics'
        import ostap.logger.table as T
        return T.table ( rows , title = title , prefix = prefix , alignment = 'll' )

# =============================================================================
## @class AddPidGen
#  parallel adding of resmapled PID reposnce  for looong TChains
#  @see ostap/trees/trees.py
class AddPidGen(Task) :
    """Add new resampled PID responce to loooong TChain in parallel
    """
    def __init__          ( self             ,
                            newpid           ,
                            pidgen           , 
                            seed     = None  , 
                            variants = False , 
                            **kwargs         ) :
        
        self.__newpid   = newpid 
        self.__seed     = seed 
        self.__variants = variants 
        self.__output   = ()
        self.__pidgen   = pidgen 
        
    def initialize_local  ( self )                : self.__output = () 
    def initialize_remote ( self , jobid = -1   ) : self.__output = () 
    def process           ( self , jobid , tree ) :
        
        from ostap.trees.trees import active_branches 
        
        
        files = []
        
        ch       = tree.chain
        ## branches = self.__pidgen.branches( ch ) 
        ##with active_branches ( ch , *branches ) : 
        nc = runPidGen ( ch                         , 
                         pidgen   = self.__pidgen   ,
                         newpid   = self.__newpid   ,
                         seed     = self.__seed     ,
                         silent   = True            , 
                         variants = self.__variants )
        
        for f in tree.files :
            if not f in files : files.append ( f )

        ## list of processed  files
        self.__output = tuple ( files )
        
        return self.__output 
        
    ## merge results/datasets 
    def merge_results( self , result , jobid = -1 ) :
        
        if not self.__output : self.__output = result
        else                 :
            processed = sorted ( self.__output + result ) 
            self.__output = tuple ( processed ) 
            
    ## get the results 
    def results ( self ) : return self.__output


# =============================================================================
## The function to add resampled variable into TTree/TChain
#  @param tree input TTree/TChain to be updated
#  @param pidgen configured PidGen object
#  @param newpid the name of new resampled PID variable
#  @param seed   the seed for `ROOT.gRandom`
#  @param silent silent processing?
#  @code
#  pidgen = PidGen ( ... ) ## configure PidGenObject
#  tree   = ....
#  runPidGen ( tree , pidgen , 'new_ProbNNK' ) 
#  @endcode
def runPidGen ( tree             ,   ## initial tree/chain to be updated 
                pidgen           ,   ## PidGen object 
                newpid           ,   ## name of new PID variable 
                seed     = None  ,   ## random seed
                silent   = False ,   ## silent ?
                variants = False ,   ## add alterbvative models?
                parallel = False ,   ## use parallel processing?
                **kwargs         ) : ## arguments for parallel processing (WorkManager)
    """The function to add resampled variable into TTree/TChain
    - `tree`   input `ROOT.TTree/TChain` to be updated
    - `pidgen` configured `PidGen` object
    - `newpid` the name of new resampled PID variable
    - `seed`   the seed for `ROOT.gRandom` 
    - `silent` silent processing?
    
    >>> pidgen = PidGen ( ... ) ## configure PidGenObject
    >>> tree   = ....
    >>> runPidGen ( tree , pidgen , 'new_ProbNNK' ) 
    """

    assert isinstance ( tree   , ROOT.TTree ) and tree , "Invalid 'tree' argument!"
    assert isinstance ( pidgen , PidGen     )          , "Invalid 'pidgen' argument!"

    assert not newpid in tree.branches() ,"`Branch' %s already exists!" % newpid 

    old_branches = set ( tree.branches() ) | set ( tree.leaves() )


    ## parallel processing?
    if parallel and isinstance ( tree , ROOT.TChain ) and 1 < len ( tree.files() ) :
        
        from ostap.trees.trees import Chain
        ch       = Chain ( tree ) 
        cname    = tree.name
        
        ## create the task 
        task     = AddPidGen ( newpid   = newpid   ,
                               pidgen   = pidgen   ,
                               seed     = seed     ,
                               variants = variants )
        
        wmgr     = WorkManager ( silent = silent , **kwargs )
        trees    = ch.split    ( max_files = 1  )
        
        wmgr.process ( task , trees )
        
        new_chain = ROOT.TChain ( cname )
        for f in ch.files :  new_chain.Add ( f )
        
    ## sequential processing 
    else :

        if kwargs :
            logger.warning ( "runPidGen: ignore arguments : %s" % [ k for k in kwargs.keys( ) ] )
            
        from   ostap.utils.utils import root_random_seed
        with root_random_seed ( seed ) :
            
            from   ostap.math.make_fun  import make_fun3
            the_function = ( make_fun3 ( pidgen ) , 
                             pidgen.pt_log_var    ,
                             pidgen.eta_var       ,
                             pidgen.ntrk_log_var  )
            
            ## add new branch        
            new_chain = tree.add_new_branch ( newpid               ,
                                              the_function         , 
                                              verbose = not silent ,
                                              report  = False      )
            
        if variants : 
            vars = pidgen.variants ()
            for var in progress_bar ( vars , silent = silent , description = ' %+d variants:' % len ( vars ) ) :                
                varpg   = PidGen ( pt_log_var   = pidgen.pt_log_var   ,
                                   eta_var      = pidgen.eta_var      ,
                                   ntrk_log_var = pidgen.ntrk_log_var ,
                                   config       = pidgen.config       ,
                                   dataset      = pidgen.dataset      ,
                                   variant      = var                  ,
                                   silent       = True                 )
                ## add new variable 
                new_chain = runPidGen ( new_chain ,
                                        varpg     ,
                                        '%s_%s' % ( newpid , var ) ,
                                        seed     = seed            ,
                                        silent   = True            ,
                                        variants = False           )                    
    ## final summary table                 
    if not silent :
        
        title = 'PidGen(%s): configuration&statistics' % pidgen.variant 
        logger.info ( '%s:\n%s' % ( title , pidgen.table ( title , prefix = '# ' ) ) )

        new_branches = set ( new_chain.branches() ) | set ( new_chain.leaves() )
        new_branches = new_branches - old_branches
        if new_branches :
            n = len ( new_branches )
            if 1 == n : title = 'Added %s branch to TTree/TChain'   % n 
            else      : title = 'Added %s branches to TTree/TChain' % n 
            table = new_chain.table ( new_branches , title = title , prefix = '# ' )
            logger.info ( '%s:\n%s' % ( title , table ) ) 
            
    return new_chain

# =============================================================================
## ditto 
run_pid_gen = runPidGen


# =============================================================================
if '__main__' == __name__ :
    
    from ostap.utils.docme import docme
    docme ( __name__ , logger = logger )
    
# =============================================================================
##                                                                      The END 
# =============================================================================
