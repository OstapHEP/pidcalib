#!/usr/bin/env python
# -*- coding: utf-8 -*-
# =============================================================================
## @file  pidcalib1.py
#  Oversimplified script to run PIDCalib machinery from Urania project with Ostap
#
#  One needs to specify a function with the interface required nby PIDCalib (see ex_fun).
#
#  And user scrips looks as:
#
#  @code
#  def PION ( particle        ,   ## INPUT 
#             daatset         ,   ## INPUT 
#             plots   =  None ,   ## UPDATE 
#             verbose = False ) :
#      ...
#      return plots
#
#  from Ostap.PidCalib import run_pid_calib 
#  run_pid_calib ( PION , 'PIDCALIB1.db')
#
#  @endcode
# 
#  @author Vanya BELYAEV Ivan.Belyaev@itep.ru
#  @date   2014-05-10
#  @attention Run-time dependency on Urania is required! 
# =============================================================================
""" Oversimplified module to run PIDCalib machinery form Urania project

One needs to specify a function with interface required by PIDCalib (see ex_func).

And user scrips looks as:

#  def PION ( particle        , ## INPUT 
#             daatset         , ## INPUT 
#             plots   =  None , ## UPDATE 
#             verbose = False ) :
#      ...
#      return plots
#
#  from pstap.pidcalib1 import run_pid_calib 
#  run_pid_calib ( PION , 'PIDCALIB1.db')  
"""
# =============================================================================
__version__ = "$Revision$"
__author__  = "Vanya BELYAEV Ivan.Belyaev@itep.ru"
__date__    = "2011-06-07"
__all__     = (
    'makeParser'  , ## make  a parser 
    'runPidCalib' , ## run pid-calib machinery
    'getDataSet'  , ## get dataset 
    'saveHistos'  , ## save the histogram historgams
    'ex_func'     , ## an example of end-user function 
    'ex_func2'    , ## another example of end-user function 
    )
# =============================================================================
from   builtins              import range
import ostap.core.pyrouts
import ostap.logger.table    as     T 
import ostap.io.zipshelve    as     DBASE
from   ostap.plotting.canvas import use_canvas
from   ostap.plotting.style  import useStyle
from   ostap.utils.utils     import wait
import ROOT, os, sys, glob, itertools   
# =============================================================================
from   ostap.logger.logger import getLogger, setLogging 
if '__main__' == __name__ : logger = getLogger ( 'ostap.pidcalib1' )
else                      : logger = getLogger ( __name__          )
# =============================================================================

if sys.version_info < ( 3 , 0 ) :
    import itertools
    itertools.zip_longest = itertools.izip_longest
    logger.warning ("patch 'zip_longest'")
    
if ( 3,0 ) <= sys.version_info :
    
    import builtins 
    sys.modules[ 'exceptions' ] = builtins
    logger.warning ("patch 'exceptions'")
    
    import pickle
    _old_load_ = pickle.load
    def _new_load_ ( what , *args, **kwargs ) :
        if 'encoding' in kwargs : return _old_load_( what , *args , **kwargs )
        else                    : _old_load_ ( what , *args , encoding = 'latin1' , **kwargs )
        
    pickle.load = _new_load_
    logger.warning ("patch 'pickle.load'")
# =============================================================================
## default directory for PIDPerfScripts 
perfscriptsroot = '/cvmfs/lhcb.cern.ch/lib/lhcb/URANIA/URANIA_v10r1/PIDCalib/PIDPerfScripts'


# ============================================================================
## prepare the parser
#  oversimplified version of parser from MakePerfHistsRunRange.py script 
#  @author Vanya BELYAEV Ivan.Belyaev@itep.ru
#  @date   2014-07-19
def makeParser () :
    """
    Prepare the parser
    - oversimplified version of parser from MakePerfHistsRunRange.py script 
    """
    import argparse, os, sys

    parser = argparse.ArgumentParser (
        formatter_class = argparse.RawDescriptionHelpFormatter,
        prog            = os.path.basename(sys.argv[0]),
        description     = """Make performance histograms for a given:
        a) stripping version <stripVersion> (e.g. '20' )
        b) magnet polarity   <magPol>       ( 'MagUp' or 'MagDown' or 'Both' )
        c) particle type <partName>         ( 'K', 'P' , 'Pi' , 'e' , 'Mu'   )  
        """ , 
        epilog =
        """To use the 'MuonUnBiased' hadron samples for muon misID studies, one of the
        following tracks types should be used instead: \"K_MuonUnBiased\", \"Pi_MuonUnBiased\"
        or \"P_MuonUnBiased\"."""
        )
    
    ## add the positional arguments
    parser.add_argument   ( 'particle'    ,
                            metavar = '<PARTICLE>'    , type=str    ,
                            choices = ('K', 'Pi', 'P' , 'e', 'Mu' ,  'P_LcfB' , 'P_TotLc' , 'P_IncLc' ) , 
                            help    = "Sets the particle type"     )
    
    parser.add_argument   ( '-s' , '--stripping'       , nargs = '+' ,
                            metavar = '<STRIPPING>'    , ## type=str ,                           
                            help    = "Sets the stripping version(s)"  )
    
    ## add the optional arguments
    parser.add_argument   ( '-p' , '--polarity'  ,
                            default = 'Both'     , 
                            metavar = '<MAGNET>' ,
                            type    = str        ,
                            choices = ( 'MagUp', 'MagDown' , 'Both' ) , 
                            help    = "Sets the magnet polarity"    )
    
    parser.add_argument   ( '-x'   , '--minRun',
                            default = 0        , 
                            dest    = "RunMin" ,
                            metavar = "NUM"    ,
                            type    = int, 
                            help    = "Sets the minimum run number to process (if applicable)")
    parser.add_argument   ( '-y', '--maxRun', 
                            dest    = "RunMax" ,
                            metavar ="NUM"     ,
                            type    = int      ,
                            default = -1       ,
                            help    = "Sets the maximum run number to process (if applicable)")
    parser.add_argument   ( '-f', '--maxFiles',
                            dest    = "MaxFiles" ,
                            metavar = "NUM"      ,
                            type    = int        ,
                            default = -1         , 
                            help    = "Sets the maximum number of calibration files to run over")
    parser.add_argument   ( '-c', '--cuts',
                            dest    = 'cuts' , 
                            metavar = 'CUTS' ,
                            default = ''     ,
                            help    = """List of cuts to apply to the calibration sample
                            prior to determine the PID efficiencies, 
                            e.g. fiducuial volume,  HASRICH, etc... 
                            """)
    parser.add_argument   ( "-o", "--outputDir",
                            dest    = "outputDir" ,
                            metavar = "DIR",
                            type    = str ,
                            default = '.' , 
                            help    = "Save the performance histograms to directory DIR "
                            "(default: current directory)")
    
    parser.add_argument   ( '--perfscripts'                , 
                            dest    = 'ppsroot'            ,
                            metavar = "PIDPERFSCRIPTSROOT" ,
                            type    = str                  , 
                            default = perfscriptsroot      ,
                            help    = "Locaiton of PIDCalib/PerfScripts package" )
    
    addGroup = parser.add_argument_group("further options")
    addGroup.add_argument ( "-q", "--quiet"           ,
                            dest    = "verbose"       ,
                            action  = "store_false"   ,
                            default = True,
                            help    = "Suppresses the printing of verbose information")
    addGroup.add_argument(
        "-z"                   ,
        "--parallel"           ,
        dest    = "Parallel"   ,
        action  = "store_true" ,
        default = False        ,
        help    = "Use parallelization" )

    return parser 


from ostap.parallel.task import   Task
# =============================================================================
## @class PidCalibTask
#  Task for the parallel processing 
class PidCalibTask(Task) :
    """Task for the parallel processing"""
    def __init__ ( self             ,
                   pidfunc          ,
                   getconfig        ,
                   verbose  = False ) :
        self.__pidfunc   = pidfunc
        self.__getconfig = getconfig  
        self.__verbose   = {} 
        
        self.__output    = ()
    def initialize_local  ( self              ) : self.__output = ()
    def initialize_remote ( self , jobid = -1 ) : self.__output = ()
    ## actual processing 
    def process ( self , jobid , index ) :
        """Actual processing"""
        ## unpack the 

        import ostap.core.pyrouts
        import ROOT
        ## from   pidcalib.pidcalib2 import getDataSet

        dataset  = getDataSet ( index   = index          ,
                                verbose = self.__verbose ,
                                **self.__getconfig       )
        
        if dataset :
            plots   = self.__pidfunc  ( particle = self.__getconfig['particle']  ,
                                        dataset  = dataset , 
                                        plots    = []  )
        else :
            plots = ()  

        self.__output = plots 
        return self.__output 

    ## get the results 
    def results (  self ) : return self.__output 

    ## merge the results 
    def merge_results  ( self , results , jobid = -1 ) :

        if results :
            if not self.__output :
                self.__output = results
            else :
                hao , hro = self.__output 
                han , hrn = results 
                
                hao.Add  ( han ) 
                hro.Add  ( hrn )
                
                self.__output = hao , hro 

# =============================================================================
## a bit modified version of DataFuncs.GetDataSet from Urania/PIDCalib/PIDPerfScripts
def getDataSet ( particle           ,
                 stripping          ,
                 polarity           ,
                 trackcuts          ,
                 index              ,
                 verbose    = False ,
                 minEntries = 1000  ) :

    from PIDPerfScripts.DataFuncs import CheckStripVer, CheckMagPol, CheckPartType  
    CheckStripVer ( stripping )
    CheckMagPol   ( polarity  )
    CheckPartType ( particle  )

    #======================================================================
    # Create dictionary holding:
    # - Reconstruction version    ['RecoVer']
    # - np.array of:
    #        - MagUp run limits   ['UpRuns']
    #        - MagDown run limits ['DownRuns']
    #======================================================================
    from PIDPerfScripts.DataFuncs import GetRunDictionary
    DataDict = GetRunDictionary ( stripping , particle , verbose = verbose )

    #======================================================================
    # Determine Mother and Workspace names
    #======================================================================
    from PIDPerfScripts.DataFuncs import GetDataSetNameDictionary
    DataSetNameDict = GetDataSetNameDictionary( particle )

    from PIDPerfScripts.DataFuncs import GetRealPartType, GetRecoVer
    particle_type = GetRealPartType ( particle  )
    RecoVer       = GetRecoVer      ( stripping )
    #
    ## new stuff:
    #
    StripVer      = stripping
    MagPolarity   = polarity
    PartType      = particle_type 

    fname_protocol = ""
    fname_query    = ""
    fname_extra    = ""

    import os
    
    ## CALIBDATAEXTRA=eoslhcb.cern.ch
    ## CALIBDATAURLPROTOCOL=root:
    ## CALIBDATASTORE=eos/lhcb/grid/prod/lhcb/calib/lhcb/calib/pid/CalibData

    CalibDataProtocol = os.getenv ( "CALIBDATAURLPROTOCOL" , 'root:'           )
    CalibDataExtra    = os.getenv ( "CALIBDATAEXTRA"       , 'eoslhcb.cern.ch' ) 

    # set the URL protocol (if applicable)
    if CalibDataProtocol is not None and CalibDataProtocol != "":
        fname_protocol = "{0}".format(CalibDataProtocol)
        
    if CalibDataExtra is not None and CalibDataExtra!="":
        fname_extra = "{0}".format(CalibDataExtra)

    vname_head = "CALIBDATASTORE" 
    fname_head = os.getenv(vname_head , 'eos/lhcb/grid/prod/lhcb/calib/lhcb/calib/pid/CalibData' )

    if verbose : 
        logger.info ('Use       CALIBDATASTORE : %s ' % fname_head        )
        logger.info ('Use CALIBDATAURLPROTOCOL : %s ' % CalibDataProtocol )
        logger.info ('Use       CALIBDATAEXTRA : %s ' % CalibDataExtra    )
            
    if fname_head is None:
        from PIDPerfScripts.Exceptions import GetEnvError
        raise GetEnvError("Cannot retrieve dataset, environmental variable %s has not been set." %vname_head)

    fname = ("{prtcol}/{extra}/{topdir}/Reco{reco}_DATA/{pol}/"
             "{mother}_{part}_{pol}_Strip{strp}_{idx}.root").format(
        prtcol=fname_protocol, extra=fname_extra,topdir=fname_head, reco=RecoVer,
        pol=MagPolarity, mother=DataSetNameDict['MotherName'],
        part=PartType, strp=StripVer, idx=index)

    merged_fname = ("{prtcol}//{extra}//{topdir}/Reco{reco}_DATA/{pol}/"
             "{mother}_{part}_{pol}_Strip{strp}_{idx}.root").format(
        prtcol=fname_protocol, extra=fname_extra,topdir=fname_head, reco=RecoVer,
        pol=MagPolarity, mother=DataSetNameDict['MotherName'],
        part=PartType, strp=StripVer, idx=index)


#    fname = ("{prtcol}{topdir}/{pol}/{part}/"
#             "{mother}_{part}_{pol}_Strip{strp}_{idx}.root{qry}").format(
#         prtcol=fname_protocol, topdir=fname_head,
#         pol=MagPolarity, mother=DataSetNameDict['MotherName'],
#         part=PartType, strp=StripVer, idx=index, qry=fname_query)

    ## if verbose:
    ##   print "Attempting to open file {0} for reading".format(fname)

    ## import ROOT 
    ## f = ROOT.TFile.Open(fname)
    ## if not f:
    ##   f = ROOT.TFile.Open ( merged_fname )

    ## fname_protocol = ""
    ## fname_query    = ""

    ## CalibDataProtocol = os.getenv ( "CALIBDATAURLPROTOCOL" )
    ## CalibDataQuery    = os.getenv ( "CALIBDATAURLQUERY"    )

    ## # set the URL protocol (if applicable)
    ## if CalibDataProtocol : fname_protocol = "{0}://".format(CalibDataProtocol)
    
    ## # set the URL query (if applicable)
    ## if CalibDataQuery    : fname_query    = "?{0}".format(CalibDataQuery)
    
    ## from PIDPerfScripts.DataFuncs import IsMuonUnBiased 
    ## vname_head = "CALIBDATASTORE" if not IsMuonUnBiased( particle ) else "MUONCALIBDATASTORE"
    
    ## fname_head = os.getenv(vname_head)
    ## if not fname_head :
    ##     raise AttributeError ("Environmental variable %s has not been set!" %vname_head )
    
    ## fname = ("{prtcol}{topdir}/Reco{reco}_DATA/{pol}/"
    ##          "{mother}_{part}_{pol}_Strip{strp}_{idx}.root{qry}").format(
    ##     prtcol=fname_protocol, topdir=fname_head, reco=DataDict['RecoVer'],
    ##     pol  = polarity     , mother=DataSetNameDict['MotherName'],
    ##     part = particle_type, strp  =stripping, idx=index, qry=fname_query)

    
    if verbose:
        logger.info( "Attempting to open file {0} for reading".format(fname) )
        
    import ostap.core.pyrouts 
    import ROOT
    ## fname = fname
    ## with  ROOT.TFile.Open(fname, 'READ' ) as f :
    ##     if not f : fname = merged_fname 
    fname = merged_fname     
    with  ROOT.TFile.Open(fname, 'READ' ) as f :
        if not f: raise IOError("Failed to open file %s for reading" %fname)
        
        wsname = DataSetNameDict['WorkspaceName']
        ws     = f.Get(wsname)
        if not ws:
            raise AttributeError ( "Unable to retrieve RooFit workspace %s" % wsname )
        
        data = ws.data('data')
        if not data:
            raise AttributeError ( "RooDataSet not found in workspace %s"    % wsname )
        
        dataset = ROOT.RooDataSet(
            'Calib_Data'
            , data.GetTitle() 
            , data
            , data.get()
            , trackcuts        
            , 'nsig_sw'
            )
        
        ws.Delete()
        if ws   : del ws

    if verbose:
        logger.info ( "DataSet:\n%s" % dataset.table ( prefix = '# ' ) )

    #======================================================================
    # Sanity test: do we have a dataset, and is it empty?
    #======================================================================
    if not dataset              : raise AttributeError ( "Failed to create datatset" )
    if not len(dataset)         : raise AttributeError ( "DataSet is empty(1)"       )
    if not dataset.sumEntries() : raise AttributeError ( "DataSet is empty(2)"       ) 
    
    #======================================================================
    # Veto ranges with insufficient statistics
    #======================================================================
    if dataset.sumEntries() < minEntries:
        logger.warning ( "Insufficient number of entries" )
        dataset.reset()
        del dataset 
        return None
    
    return dataset 


# =============================================================================
## a bit modified version of DataFuncs.GetPerfPlotList
#  from Urania/PIDCalib/PIDPerfScripts
def makePlots ( the_func         ,
                particle         ,  
                stripping        ,
                polarity         ,
                trackcuts        ,
                runMin   =     0 ,
                runMax   =    -1 ,
                verbose  =  True ,
                maxFiles =    -1 ,
                parallel = False ) :

    #**********************************************************************
    from PIDPerfScripts.DataFuncs import CheckStripVer, CheckMagPol, CheckPartType  
    CheckStripVer ( stripping )
    CheckMagPol   ( polarity  )
    CheckPartType ( particle  )
    
    #======================================================================
    # Create dictionary holding:
    # - Reconstruction version    ['RecoVer']
    # - np.array of:
    #        - MagUp run limits   ['UpRuns']
    #        - MagDown run limits ['DownRuns']
    #======================================================================
    from PIDPerfScripts.DataFuncs import GetRunDictionary
    DataDict = GetRunDictionary ( stripping , particle , verbose = verbose )

    if trackcuts and  0 < runMin : trackcuts +=' && runNumber>=%d ' % runMin
    if trackcuts and  0 < runMax : trackcuts +=' && runNumber<=%d ' % runMax
    
    #======================================================================
    # Determine min and max file indicies
    #======================================================================
    if runMax < runMin : runMax = None 
    from PIDPerfScripts.DataFuncs import GetMinMaxFileDictionary
    IndexDict = GetMinMaxFileDictionary( DataDict , polarity ,
                                         runMin   , runMax   ,
                                         maxFiles , verbose  )
    
    #======================================================================
    # Append runNumber limits to TrackCuts
    #======================================================================
    
    logger.debug ( 'Track Cuts: %s ' % trackcuts ) 

    #======================================================================
    # Declare default list of PID plots
    #======================================================================
    plots      =   []
    minEntries = 1000

    #======================================================================
    # Loop over all calibration subsamples
    #======================================================================

    mn = IndexDict [ 'minIndex' ]
    mx = IndexDict [ 'maxIndex' ]

    from ostap.utils.memory import memory
    from ostap.utils.utils  import NoContext
    
    
    if parallel :
        
        logger.info('Parallel processing %d datafiles %s %s %s ' % ( mx - mn + 1  ,
                                                                     particle     ,
                                                                     stripping    ,
                                                                     polarity   ) )
        task    = PidCalibTask ( the_func ,
                                 getconfig = { 'particle'  : particle  ,
                                               'stripping' : stripping , 
                                               'polarity'  : polarity  , 
                                               'trackcuts' : trackcuts } ,
                                 verbose = False ) 
        
        from ostap.parallel.parallel import WorkManager 
        wmgr = WorkManager   ( silent = False )
        
        wmgr.process ( task , range ( mn , mx + 1 ) )
        return task.results () 
        
        
    logger.info('Start the loop over %d datafiles %s %s %s ' % ( mx - mn + 1  ,
                                                                 particle     ,
                                                                 stripping    ,
                                                                 polarity   ) )
    
    from ostap.utils.progress_bar import progress_bar
    for index in progress_bar ( range ( mn , mx + 1 ) ) :
        
        manager = memory() if verbose else NoContext()
        with manager :

            dataset = getDataSet ( particle  ,
                                   stripping ,
                                   polarity  ,
                                   trackcuts , 
                                   index     ,
                                   verbose = verbose )
            
            if not dataset : continue 

            new_plots = the_func ( particle ,
                                   dataset  ,
                                   plots    ,
                                   verbose  )
            
            if not plots :  plots = new_plots
            else         :
                for oh , nh in zip ( plots , new_plots ) :
                    oh.Add ( nh ) 
                    
            
            dataset.reset  ()
            if dataset : del dataset
            

    return plots 

# ====================================================================
## the basic function:
#  oversimplified version of MakePerfHistsRunRange.py script from Urania/PIDCalib
#  @author Vanya BELYAEV Ivan.Belyaev@itep.ru
#  @date   2014-07-19
def runPidCalib ( the_func        ,
                  particle         ,  
                  stripping        ,
                  polarity         ,
                  trackcuts        ,
                  RunMin   =  0    ,
                  RunMax   = -1    ,
                  MaxFiles = -1    ,
                  verbose  = True  ,
                  parallel = False ,
                  db_name  = ''    ,
                  **config         ) :
    """ The basic function:
    - oversimplified version of MakePerfHistsRunRange.py script from Urania/PIDCalib 
    """
    #
    ## perform some arguments check
    #
        
    ## 1) check the stripping version
    from PIDPerfScripts.DataFuncs import CheckStripVer 
    CheckStripVer ( stripping )
    
    ## 2) set the magnet polarity  [not-needed, since embedded into parser]
    from PIDPerfScripts.DataFuncs import CheckMagPol
    CheckMagPol   ( polarity  )
    
    ## 3) set the particle name [not-needed, since embedded into parser]
    from PIDPerfScripts.DataFuncs import CheckPartType
    CheckPartType ( particle  )
    
    
    ## a bit strange treatment of runMax in PIDCalib :-(
    
    #
    ## finally call the standard PIDCalib machinery with user-specified function
    #
    histos =  makePlots ( the_func                    ,
                          particle                    ,  
                          stripping                   ,
                          polarity                    ,
                          trackcuts                   ,
                          runMin    = RunMin          ,
                          runMax    = RunMax          ,
                          verbose   = verbose         ,
                          maxFiles  = MaxFiles        ,
                          parallel  = parallel        )

    if db_name :
        import ostap.io.zipshelve as DBASE
        try : 
            with DBASE.open ( db_name ) as db :
                if verbose : logger.info("Save data into '%s'" % db_name )
                ##
                key = '%s/%s/%s' %  ( stripping , polarity , particle ) 
                db [ key ] = histos
                ##
                if verbose : db.ls()
                ##
        except :
            logger.error ( "Cannot save results to '%s'" % db_name )
            raise 
            
    return histos

# =============================================================================
## save histograms into the output file
#  @code
# 
#  histos = runPidCalib ( .... )
#  saveHistos ( histos , '' , **config )
# 
#  @endcode 
#  @author Vanya BELYAEV Ivan.Belyaev@itep.ru
#  @date   2014-07-19
def saveHistos  (  histos         ,
                   fname   = ''   ,
                   verbose = True , **config ) :
    """Save histograms into the output file """
    ##
    verbose   = config.get('verbose')
    if   not fname and not config : fname = "PIDHistos.root"
    elif not fname and     config :
        fname = "PIDHistos_{part}_Strip{strp}_{pol}.root".format(
            part = config [ 'particle'  ] ,
            strp = config [ 'stripping' ] ,
            pol  = config [ 'polarity'  ] )

    if verbose : logger.info ( "Saving performance histograms to %s" %fname )
    
    import ROOT 
    import ostap.core.pyrouts  
    import ostap.io.root_file 
    
    with ROOT.TFile.Open ( fname, "RECREATE") as outfile :

        outfile.cd() 
        ## save all histograms 
        for h in histos : h.Write() 
        if verbose : outfile.ls() 
        
    return fname 

# =============================================================================
## the example of the actual function that builds the histos
#
#  In this example it builds two histograms:
#  - accepted events
#  - rejected events
#  the efficiency historgam can be later build in Ostap as :
#  @code
#
#  eff = 1/(1 + h_rej/h_acc)
# 
#  @endcode
#  For dataset structure and variable names see
#  @see https://twiki.cern.ch/twiki/bin/view/LHCb/PIDCalibPackage
#  @author Vanya BELYAEV Ivan.Belyaev@itep.ru
#  @date   2014-07-19
def  ex_func ( particle          ,
               dataset           ,
               plots     = None  , 
               verbose   = False ) :
    """The example of the actual function that build histos
    
    In this example it builds two histograms:
    - accepted events
    - rejected events
    
    For dataset structure and variable names see:
    https://twiki.cern.ch/twiki/bin/view/LHCb/PIDCalibPackage
    
    The efficiency historgam can be later build in Ostap as :
    
    >>> h_acc = ...
    >>> h_rej = ...
    >>> eff = 1/(1 + h_rej/h_acc)
    """
    
    ## we need ROOT and Ostap machinery!    
    import ROOT
    from   ostap.core.pyrouts import hID 
    
    # 1) define PID-cut and its negation  
    #    For dataset structure and variable names see:
    #    https://twiki.cern.ch/twiki/bin/view/LHCb/PIDCalibPackage
    accepted = 'Pi_ProbNNpi>0.5' ## note variable names 
    rejected = 'Pi_ProbNNpi<0.5' ## note variable names 
    
    # 2) prepare the histogtrams 
    hA = ROOT.TH2D ( hID () , 'Accepted(%s)'% accepted , 15 , 0 , 150000 , 10 , 2 , 5 ) ; h1.Sumw2()
    hR = ROOT.TH2D ( hID () , 'Rejected(%s)'% rejected , 15 , 0 , 150000 , 10 , 2 , 5 ) ; h2.Sumw2()
    
    # 3) fill the historgams with 'accepted' and 'rejected' events
    #    For dataset structure and variable names see:
    #    https://twiki.cern.ch/twiki/bin/view/LHCb/PIDCalibPackage
    
    vlst = ROOT.RooArgList ()
    vlst.add ( dataset.Pi_P   ) ## note variable names 
    vlst.add ( dataset.Pi_Eta ) ## note variable name 
    
    hA = DataSet.fillHistogram ( hA , vlst , accepted ) ## fill histo 
    hR = DataSet.fillHistogram ( hR , vlst , rejected ) ## fill histo
    
    #
    ## and now update the output 
    #
    
    if not plots :

        hA.SetName ( hA.GetTitle() )
        hR.SetName ( hR.GetTitle() )
        
        plots     = [ hA , hR ] ## "Accepted" & "Rejected" histograms 

    else         :
        
        plots[0] += hA          ## "Accepted" histogram 
        plots[1] += hR          ## "Rejected" histogram
        
        hA.Delete ()
        hR.Delete ()
        if hA : del hA
        if hR : del hR
        
    if verbose :
        
        logger.info ( 'Accepted histo: %s' % plots[0].stat() )
        logger.info ( 'Rejected histo: %s' % plots[1].stat() )
            
    return plots


# =============================================================================
## the example of the actual function that builds the histos
#
#  In this example it builds two histograms:
#  - accepted events
#  - rejected events
#  the efficiency historgam can be later build in Ostap as :
#  @code
#
#  eff = 1/(1 + h_rej/h_acc)
# 
#  @endcode
#  For dataset structure and variable names see
#  @see https://twiki.cern.ch/twiki/bin/view/LHCb/PIDCalibPackage
#  @author Vanya BELYAEV Ivan.Belyaev@itep.ru
#  @date   2014-07-19
def  ex_func2 ( particle         ,
                dataset          ,
                plots    = None  ,
                verbose  = False ) :

    ## we need ROOT and Ostap machinery!    
    import ROOT
    from  ostap.core.pyrouts  import hID
    from  ostap.histos.histos import h3_axes  

    ## the main:
    accepted = 'K_ProbNNK>0.1'    ## ACCEPTED sample 
    rejected = 'K_ProbNNK<0.1'    ## REJECTED sample 

    ## variables for the historgrams 
    vlst = ROOT.RooArgList ()
    vlst.add ( dataset.K_P   )
    vlst.add ( dataset.K_Eta )
    vlst.add ( dataset.nTracks )

    ## binning    
    pbins    = [ 3.2  , 6  , 9  ,  15 , 20  , 30  , 40 , 50 , 60 , 80 , 100 , 120 , 150 ]
    pbins    = [ p*1000 for p in pbins ]
    
    hbins    = [ 2.0 , 2.5 , 3.0 , 3.5 , 4.0 , 4.5 , 4.9 ]
    tbins    = [0, 150 , 250 , 400 , 1000]

    ## book histogams
    
    ha       = h3_axes ( pbins , hbins , tbins , title = 'Accepted(%s)' % accepted ) 
    hr       = h3_axes ( pbins , hbins , tbins , title = 'Rejected(%s)' % rejected )

    ## fill them
    
    ha = dataset.fillHistogram ( ha , vlst , accepted )
    hr = dataset.fillHistogram ( hr , vlst , rejected )

    if not plots :
        
        ha.SetName ( ha.GetTitle() )
        hr.SetName ( hr.GetTitle() )
        
        plots = [ ha , hr ]           ## create plots 
        
    else         :

        plots [0] += ha               ## update plots 
        plots [1] += hr               ## update plots 
        
        ha.Delete ()
        hr.Delete ()
        if ha : del ha
        if hr : del hr
        
    return plots

# =============================================================================
## run pid-calib procedure
def run_pid_calib ( FUNC , db_name = 'PID_eff.db' , args = [] ) :
    """ Run PID-calib procedure 
    """
    
    parser  = makeParser        ()
    if not args :
        import sys
        args =[ a for a in sys.argv[1:] if '--' != a ]  
    config    = parser.parse_args ( args )


    ## 1) check the PIDCalibPerfScript
    assert config.ppsroot and os.path.exists ( config.ppsroot ) and os.path.isdir ( config.ppsroot ) , \
           'Invalid specification for PIDPERFSCRIPTROOT %s' % config.ppsroot
    ## 2) check existence of pickling files
    pattern  = '%s/pklfiles/Stripping*/*.pkl' % config.ppsroot 
    assert any ( i for i in glob.iglob ( pattern ) ) , \
           "No pickle directories/files are found in %s" % pattern
    ## 3) play with environment
    os.environ['PIDPERFSCRIPTSROOT'] = config.ppsroot  
    sys.path.insert ( 0 , os.path.join ( config.ppsroot, 'python' ) ) 

    try :
        import PIDPerfScripts
        logger.info  ( "Module PIDPerfScripts is found at %s" % PIDPerfScripts.__file__ ) 
    except :
        logger.fatal ( "Module PIDPerfScripts is not accessible!")
        raise
    

    polarity  =  config.polarity
    if 'Both' == polarity  : polarity  = [ 'MagUp'  , 'MagDown' ]
    else                   : polarity  = [ polarity ]

    stripping =  config.stripping
    ##if not stripping : stripping = [ '21' , '21r1' ]
    ##if not stripping : stripping = [ '20' , '20r1' ]
    
    particle  = config.particle
    
    particles = [ particle ]
    if   'P' == particle :
        if   '20' in stripping or '20r1' in stripping :
            particles = [ 'P_IncLc' ] + [ particle ]            
            logger.info ( 'Use reduced set of protons species %s ' % particles )
        elif '21' in stripping or '21r1' in stripping :
            particles = [ 'P_LcfB' , 'P_TotLc' , 'P_IncLc' ] + particles
            logger.info ( 'Use all species of protons %s ' % particles )

    logger.info ( 'Stripping versions: %s' % stripping )  
    logger.info ( 'Magnet polarities : %s' % polarity  )  
    logger.info ( 'Particles         : %s' % particles )
    
    table = [ ('' , 'Value' )]
    
    row = 'Particle'  , str ( particles )
    table.append ( row )

    row = 'Stripping' , str ( stripping )
    table.append ( row )

    row = 'Polarity'   , str ( polarity  )
    table.append ( row )

    if 0 < config.RunMin <= config.RunMax :        
        row = 'Run min/max'   ,  "%s/%s" % ( config.RunMin , config.RunMax )
        table.append ( row )

    if 0 < config.MaxFiles :
        row = 'MaxFiles'           , '%s' % config.MaxFiles 
        table.append ( row )

    if config.cuts : 
        row = 'Additonal cuts'     , '%s' % config.cuts  
        table.append ( row )

    if '.' != config.outputDir : 
        row = 'Output directory'   , '%s' % config.outputDir
        table.append ( row )

    
    row = 'Verbose'                  , '%s' % config.verbose
    table.append ( row )

    row = 'PIDPERFSCRIPTSROOT' ,  os.getenv ( 'PIDPERFSCRIPTSROOT' , '' )
    table.append ( row )
    
    title = 'PIDCalib configuration'
    import ostap.logger.table as T
    table = T.table ( table , title = 'title', prefix = '# ' , alignment = 'lw' , indent = '' )
    logger.info ( 'title\n%s' % table ) 
  
    ## results  
    results = {} 
    hfiles  = []
    ## loop over the magnet polarities 
    for m in polarity :
        
        ## loop over stripping versions 
        for s in stripping :
            
            ## loop over calibration techniques/particle species 
            for p in particles :

                key = '%s/%s/%s' %  ( s , m , p ) 
                
                histos = runPidCalib ( FUNC        ,
                                       p           ,
                                       s           ,
                                       m           ,
                                       config.cuts , 
                                       RunMin   = config.RunMin      ,
                                       RunMax   = config.RunMax      ,
                                       MaxFiles = config.MaxFiles    ,
                                       verbose  = config.verbose     ,
                                       parallel = config.Parallel    ,
                                       db_name  = db_name            )
                
                results [ key ] = histos 
                ## hfile = saveHistos  ( histos              ,
                ##                       particle  = p       ,
                ##                       stripping = s       ,
                ##                       polarity  = m       ,
                ##                       verbose   = config.verbose )                 
                ## hfiles.append ( hfile )

    total = {}
    for k in results :
        
        tkey , _ , _  = k.rpartition( '/' )
        tkey = '%s/TOTAL_%s' % ( tkey , config.particle  )

        hacc , hrej = results [ k ] 
        if not tkey in total : total[tkey] = hacc.clone() , hrej.clone()
        else : 
            a , r = total [ tkey ]
            a += hacc
            r += hrej
            total [ tkey ] = a , r
            
    header  = ( 'Sample' , '#accepted [10^3]' , '#rejected [10^3]' , '<glob-eff> [%]'  , '<diff-eff> [%]' , 'min [%]' , 'max [%]' )
    report  = [ header ]

    ## store "combined" results, (just for illustration) and collect statistics 
    if results and db_name and os.path.exists ( db_name ) :
        with DBASE.open ( db_name ) as db :
            ## store total results 
            for key in total: db [ key] = total [ key ]
            
            ## update some results & make statistics 
            for key in itertools.chain ( results , total ) :
                
                if not key in db : continue
                
                hacc , hrej = db [ key ]
                heff = 1.0 / ( 1 + hrej / hacc )
                db [ '%s:efficiency'% key ] = heff  ## ATTENTION! store it! 
                      
                na   = hacc.accumulate () / 1000
                nr   = hrej.accumulate () / 1000
                
                heff = 100. / (1. + hrej / hacc )
                eeff = 100. / (1. + nr   / na  )
                hst  = heff.stat()
                
                row = key , \
                      na.toString   ( '%10.2f +/- %-6.2f' ) ,           \
                      nr.toString   ( '%10.2f +/- %-6.2f' ) ,           \
                      eeff.toString ( '%6.2f +/- %-5.2f'  ) ,           \
                      '%6.2f +/- %-5.2f' % ( hst.mean() , hst.rms() ) , \
                      '%+6.2f'           %   hst.min()      ,           \
                      '%+6.2f'           %   hst.max()
                
                report.append ( row )

            db [ 'TOTAL_%s:conf'      % config.particle ] = config
                        
            db.ls() 
            
    table = T.table ( report , title = 'Performance for %s processed samples' % len ( results ) , prefix = '# ')
    logger.info ( 'Performance for %s processed samples:\n%s' % ( len ( results ) , table ) )
    
    if total and config.verbose and os.path.exists ( db_name ) and not ROOT.gROOT.IsBatch () :
        
        ROOT.gStyle.SetPalette ( 53 )
        
        with DBASE.open ( db_name , 'r') as db:
            for k in sorted ( total  ) :
                
                tag = k + ':efficiency'
                if not tag in db : continue
                
                heff = db.get ( tag , None )
                if     not heff                           : continue
                elif   not isinstance ( heff , ROOT.TH1 ) : continue 
                elif   3 == heff.dim () :
                    for i in range ( 1 , heff.nbinsz () ) :
                        title = 'Sample %25s, 2D-efficiency, z-slice #%d' % ( k , i + 1 ) 
                        zs = heff.sliceZ ( i )
                        with wait ( 2 ) , use_canvas ( title ) , useStyle ( 'Z' ) : zs.draw('colz') 
                elif 2 == heff.dim () :
                    for i in range ( 1 , heff.nbinsy () ) :
                        title = 'Sample %25s, 1D-efficiency, y-slice #%d' % ( k , i + 1 ) 
                        ys = heff.sliceY ( i )
                        with wait ( 2 ) , use_canvas ( title ) : ys.draw('') 
                elif 1 == heff.dim () :
                    title = 'Sample %25s, 1D-efficiency' % ( k ) 
                    with wait ( 2 ) , use_canvas ( title ) : heff.draw() 

    
    logger.info('Produced files: ')
    for i in hfiles : logger.info ( i )  


# =============================================================================
if '__main__' == __name__ :
    
    from ostap.utils.docme import docme
    docme ( __name__ , logger = logger )

    run_pid_calib ( None , args = [ '-h'] )
    
# =============================================================================
##                                                                      The END
# =============================================================================
