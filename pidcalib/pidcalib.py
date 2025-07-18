#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ================================================================================
## @file  pidcalib.py
#  A tiny wrapper for PIDCalib2 machinery 
#  @see https://pypi.org/project/pidcalib2
# 
# *********************************************************************************
# *                                                                               *
# *  ooooooooo.    o8o        .o8    .oooooo.             oooo   o8o   .o8        * 
# *  `888   `Y88.  `"'       "888   d8P'  `Y8b            `888   `"'  "888        * 
# *   888   .d88' oooo   .oooo888  888           .oooo.    888  oooo   888oooo.   *
# *   888ooo88P'  `888  d88' `888  888          `P  )88b   888  `888   d88' `88b  *
# *   888          888  888   888  888           .oP"888   888   888   888   888  *
# *   888          888  888   888  `88b    ooo  d8(  888   888   888   888   888  * 
# *  o888o        o888o `Y8bod88P"  `Y8bood8P'  `Y888""8o o888o o888o  `Y8bod8P'  *
# *                                                                               *
# *********************************************************************************
#
# For the given particle PID-cut/criterion,
# one produces  1D,2D or 3D efficiency histograms for data-taking
# perions/samples and magnet polarities.
#
#  - The usage is fairly transparent
#
# The elementary processing request is defined using the helper classes
# <code>PARTICLE_1D</code>, <code>PARTICLE_2D</code> or<code>PARTICLE_3D</code>
# for 1D, 2D and 3D efficiencies.  
#
# - The binnig schemes are provided and (template) histogram during thr creation of the procesing requests
#
# @code
#
# h1D = ROOT.TH1D ( ... )#
# request1 = PARTICLE_1D ( 'Pi'                             , ## particle type 
#                          'probe_MC15TuneV1_ProbNNpi>0.5 ' , ## criterion to be tested
#                          'Turbo18'                        , ## data sample
#                          'up'                             , ## magnet polarity
#                          ""                               , ## additional cuts
#                           h1D                             , ## 1D template histogram
#                         'log10(probe_P/1000)'             ) ## the x-axis variable
#
# h2D = ROOT.TH2D ( ... ) 
# request2 = PARTICLE_2D ( 'Pi'                             , ## particle type 
#                         'probe_MC15TuneV1_ProbNNpi>0.5 '  , ## criterion to be tested
#                         'Turbo18'                         , ## data sample
#                         'up'                              , ## magnet polarity
#                          ""                               , ## additional cuts
#                          h2D                              , ## 1D template histogram
#                         'log10(probe_P/1000)'             , ## the x-axis variable
#                         'probe_ETA'                       ) ## the y-axis variable
# 
# h3D = ROOT.TH3D ( ... ) 
# request3 = PARTICLE_3D ( 'Pi'                             , ## particle type 
#                         'probe_MC15TuneV1_ProbNNpi>0.5 '  , ## criterion to be tested
#                         'Turbo18'                         , ## data sample
#                         'up'                              , ## magnet polarity
#                          ""                               , ## additional cuts
#                          h3D                              , ## 1D template histogram
#                         'log10(probe_P/1000)'             , ## the x-axis variable
#                         'probe_ETA'                       , ## the y-axis variable
#                         'nSPDhits'                        ) ## the z-axis variable
# @endcode 
#
# - The requests have only one important method: <code>process<./code>:
#
# @code
# 
# efficiency , accepted, rejected = request.process ( silent    = False , ## silent processing  ?
#                                                     progress  = True  ) ## show progress bar ?
# @endcode
# 
# As result one gets a triplet of historgams:
#
# - efficiency : the histogram representing the efficienct for the specified PID criterion
# - accepted   : the distribution for events accepted by the PID criterion
# - rejected   : the distribution for events rejected by the PID criterion
#
# Accepted and rejected histogram can be summed for diffrent samples
# and the combined efficiency can be recalculatedas:
#  
#   efficiency = 1 / ( 1 + rejected / accepted )
#
# To speedup athe processing for multicore machines one can use
# `use_frame` and/or `parallel` directives:
# 
# @code
#
# ## use RDataFrame machinery  
# efficiency , accepted, rejected = request.process ( silent    = False , ## silent processing ?
#                                                     progress  = True  , ## show progress bar ?
#                                                     use_frame = True  , ## use RDataFrame
#
# ## use parallel processing 
# efficiency , accepted, rejected = request.process ( silent    = False , ## silent processing  ?
#                                                     progress  = True  , ## show progress bar  ?
#                                                     parallel  = True  , ## parallel processing ?
# @endcode
#
#
# At the end  all results can be saved into database:
#
# @code
# import ostap.io.zipshelve as DBASE 
#
# requests = [
#     PARTICLE_1D (... ) , 
#     PARTICLE_1D (... ) , 
#     PARTICLE_1D (... ) , 
#     PARTICLE_1D (... ) , 
#     PARTICLE_1D (... ) ]
#
# for request in requests :
#    results = request.process ( ... )
#    with DBASE.open ( 'results.db' ) as db :
#       tag = 'results:%s/%s/%s' % ( request.particle , requests.sample, request.magnet )
#       db [ tag ] = results 
#
# with DBASE.open ( 'results.db' , 'r' ) as db : db.ls() 
# 
# endcode
# 
#  @author Vanya BELYAEV Ivan.Belyaev@cern.ch
#  @date   2014-05-10
# =======================================================================================

""" A tiny wrapper for PIDCalib2 machinery 
 - see https://pypi.org/project/pidcalib2

  *********************************************************************************
  *                                                                               *
  *  ooooooooo.    o8o        .o8    .oooooo.             oooo   o8o   .o8        * 
  *  `888   `Y88.  `"'       "888   d8P'  `Y8b            `888   `"'  "888        * 
  *   888   .d88' oooo   .oooo888  888           .oooo.    888  oooo   888oooo.   *
  *   888ooo88P'  `888  d88' `888  888          `P  )88b   888  `888   d88' `88b  *
  *   888          888  888   888  888           .oP"888   888   888   888   888  *
  *   888          888  888   888  `88b    ooo  d8(  888   888   888   888   888  * 
  *  o888o        o888o `Y8bod88P"  `Y8bood8P'  `Y888""8o o888o o888o  `Y8bod8P'  *
  *                                                                               *
  *********************************************************************************

  For the given particle PID-cut/criterion,
  one produces  1D,2D or 3D efficiency histograms for data-taking
  perions/samples and magnet polarities.

  - The usage is fairly transparent
 
  The elementary processing request is defined using the helper classes
  `PARTICLE_1D`, `PARTICLE_2D` or `PARTICLE_3D` for 1D, 2D and 3D efficiencies.  

 - The binnig schemes are provided and (template) histogram during thr creation of the procesing requests

 >>> h1D = ROOT.TH1D ( ... )#
 >>> request1 = PARTICLE_1D ( 'Pi'                             , ## particle type 
 ...                          'probe_MC15TuneV1_ProbNNpi>0.5 ' , ## criterion to be tested
 ...                          'Turbo18'                        , ## data sample
 ...                          'up'                             , ## magnet polarity
 ...                          ""                               , ## additional cuts
 ...                          h1D                             , ## 1D template histogram
 ...                         'log10(probe_P/1000)'             ) ## the x-axis variable


 >>> h2D = ROOT.TH2D ( ... ) 
 >>> request2 = PARTICLE_2D ( 'Pi'                             , ## particle type 
 ...                          'probe_MC15TuneV1_ProbNNpi>0.5 '  , ## criterion to be tested
 ...                         'Turbo18'                         , ## data sample
 ...                         'up'                              , ## magnet polarity
 ...                          ""                               , ## additional cuts
 ...                          h2D                              , ## 1D template histogram
 ...                         'log10(probe_P/1000)'             , ## the x-axis variable
 ...                         'probe_ETA'                       ) ## the y-axis variable


 >>> h3D = ROOT.TH3D ( ... ) 
 >>> request3 = PARTICLE_3D ( 'Pi'                             , ## particle type 
 ...                         'probe_MC15TuneV1_ProbNNpi>0.5 '  , ## criterion to be tested
 ...                         'Turbo18'                         , ## data sample
 ...                         'up'                              , ## magnet polarity
 ...                          ""                               , ## additional cuts
 ...                          h3D                              , ## 1D template histogram
 ...                         'log10(probe_P/1000)'             , ## the x-axis variable
 ...                         'probe_ETA'                       , ## the y-axis variable
 ...                         'nSPDhits'                        ) ## the z-axis variable


 - The requests have only one important method: `process`

 
 >>> efficiency , accepted, rejected = request.process ( silent    = False , ## silent processing  ?
 ...                                                     progress  = True  ) ## show progress bar ?

  As result one gets a triplet of historgams:

   - efficiency : the histogram representing the efficienct for the specified PID criterion
   - accepted   : the distribution for events accepted by the PID criterion
   - rejected   : the distribution for events rejected by the PID criterion

 Accepted and rejected histogram can be summed for diffrent samples
 and the combined efficiency can be recalculatedas:
  
      efficiency = 1 / ( 1 + rejected / accepted )

 To speedup athe processing for multicore machines one can use
 `use_frame` and/or `parallel` directives:
 
 >>> ## use RDataFrame machinery  
 >>> efficiency , accepted, rejected = request.process ( silent    = False , ## silent processing ?
 ...                                                     progress  = True  , ## show progress bar ?
 ...                                                     use_frame = True  , ## use RDataFrame


 >>> ## use parallel processing 
 >>> efficiency , accepted, rejected = request.process ( silent    = False , ## silent processing  ?
 ...                                                     progress  = True  , ## show progress bar  ?
 ...                                                     parallel  = True  , ## parallel processing ?


 At the end  all results can be saved into database 

 >>> import ostap.io.zipshelve as DBASE 

 >>> ## create requests fr different particles, smaples, polarities , ....
 >>> requests = [
 ...     PARTICLE_1D (... ) , 
 ...     PARTICLE_1D (... ) , 
 ...     PARTICLE_1D (... ) , 
 ...     PARTICLE_1D (... ) , 
 ...     PARTICLE_1D (... ) ]

 >>> ## process all requests 
 >>> for request in requests :
 ...     results = request.process ( ... )
 ...     with DBASE.open ( 'results.db' ) as db :
 ...         tag = 'results:%s/%s/%s' % ( request.particle , requests.sample, request.magnet )
 ...         db [ tag ] = results 
 
 >>> with DBASE.open ( 'results.db' , 'r' ) as db : db.ls() 

"""
# =============================================================================
__version__ = "$Revision$"
__author__  = "Vanya BELYAEV Ivan.Belyaev@cern.ch"
__date__    = "2014-05-10"
__all__     = (
    'PARTICLE_1D' , ## description of elementary request to make  1D-efficiency
    'PARTICLE_2D' , ## description of elementary request to make  1D-efficiency
    'PARTICLE_3D' , ## description of elementary request to make  1D-efficiency
)    
# =============================================================================
from   abc                   import ABC, abstractmethod
from   ostap.utils.basic     import typename
import ostap.trees.trees
import ostap.histos.histos
import pidcalib2.pid_data 
import ROOT
# =============================================================================
from ostap.logger.logger import getLogger
logger = getLogger('ostap.pidcalib')
# =============================================================================
MAX_FILES = -1 
# =============================================================================
## The abstract base class for processing
#  - It has only one essential method: <code>process</code>
#  - It should return a tuple of three thistogram: efficiency, accepted and rejected 
class PARTICLE (ABC)  :
    """ The abstract base class for processing
    - It has only one   essential method: `process`
    - It should return a tuple of three thistogram: efficiency, accepted and rejected 
    """
    def __init__ ( self      ,
                   particle  ,
                   criterion , 
                   sample    ,
                   magnet    ,
                   cuts      = '' ) :
        
        self.__particle  = particle
        self.__criterion = criterion
        self.__sample    = sample
        self.__magnet    = magnet
        self.__cuts      = ROOT.TCut ( cuts ) 
        
        ## get the calibration sample 
        self.__data = pidcalib2.pid_data.get_calibration_sample  (
            self.__sample   ,
            self.__magnet   ,
            self.__particle ,
            None            , #
            None            )
        
        ##
        ## MAX_FILES if isinstance ( MAX_FILES , int ) and 0 < MAX_FILES else None )
        ##
        
        assert self.__data , "Invalid sample/magnet/particle combination %s/%s/%s" % ( self.__sample   ,
                                                                                       self.__magnet   ,
                                                                                       self.__particle )
        
        ## update the CUTS
        data_cuts = self.__data.get ( 'cuts' , [] )
        if data_cuts : logger.attention ( "The `cuts` are defined with dataset: %s" % str ( data_cuts ) )  
        for cut in self.__data.get ( 'cuts', [] ) : self.__cuts &= cut
        
        ## get sWeightvariable 
        self.__sWeight    = self.__data.get ( 'sweight_branch' , 'probe_sWeight' )
        
        self.__tree_paths = pidcalib2.pid_data.get_tree_paths (
            self.__particle , 
            self.__sample   ,
            self.__data [ "tuple_names" ][ self.__particle ] \
            if "tuple_names" in self.__data and self.__particle in self.__data ["tuple_names" ] else None )

    @abstractmethod 
    def histogram ( self ) :
        """`histogram` : get the (template) histogram 
        """
        pass 
    
    @abstractmethod 
    def variables ( self ) :
        """`variables` : axes for template histogram
        """
        pass 

    @property
    def particle ( self  ) :
        """`particle` : Particle type to process """
        return self.__particle
    
    @property
    def sample ( self  ) :
        """`sample` : Data sample to process """
        return self.__sample
    
    @property
    def magnet ( self  ) :
        """`magnet` : Magner polarity"""
        return self.__magnet
    
    # =========================================================================
    ## Process the request 
    #
    #  The actual methdo that loops over calibration smaples and  produce a tripet of histograms
    #  - efficiency : the efficiency for the specified criterion 
    #  - accepted   : distribution for events accepted by criterion
    #  - rejected   : distribution for events rejected by criterion
    #
    #  @see data_efficiency 
    def process ( self              ,
                  progress  = True  ,
                  silent    = False , 
                  use_frame = False ,
                  parallel  = False ) :
        """ Process the samples
        
        The actual methdo that loops over calibration smaples and  produce a tripet of histograms
        - efficiency : the efficiency for the specified criterion 
        - accepted   : distribution for events accepted by criterion
        - rejected   : distribution for events rejected by criterion
        
        see `data_efficiency` 
        """
        efficiency, accepted, rejected = None , None , None

        from ostap.stats.statvars import data_efficiency 

        if use_frame and not ROOT.ROOT.IsImplicitMTEnabled () : ROOT.ROOT.EnableImplicitMT()
            
        ## explicit loop over the defiend paths 
        for tree_path in self.__tree_paths :
            
            chain = ROOT.TChain ( tree_path )
            for fname in self.__data['files'] : chain.Add  ( fname ) 
            
            if not silent : logger.info ( 'Processing: %s/%s/%s:%s' % ( self.__particle ,
                                                                        self.__sample   ,
                                                                        self.__magnet   ,
                                                                        tree_path       ) ) 
            _ , a , r = data_efficiency ( chain            ,
                                          self.__criterion ,
                                          self.histogram() ,
                                          self.variables() ,
                                          self.__cuts      ,
                                          weight    = self.__sWeight ,
                                          use_frame = use_frame      ,
                                          parallel  = parallel       ,
                                          progress  = progress       )
            
            if  accepted is None : accepted  = a
            else                 : accepted += a
            
            if  rejected is None : rejected  = r 
            else                 : rejected += r

            efficiency = 1 / ( 1 + rejected / accepted )
            
        return efficiency , accepted , rejected


# =============================================================================
## @class PARTICLD_1D
#  The actual function to produce 1D efficiency, e.g. as function of P
#  @author Vanya BELYAEV Ivan.Belyaev@itep.ru
#  @date   2014-05-10
class PARTICLE_1D(PARTICLE):
    """ The actual function to produce 1D efficiency, e.g. as function of P
    """    
    ## create the object
    def __init__ ( self       ,
                   particle   ,
                   criterion  ,
                   sample     ,
                   magnet     ,
                   cuts       ,
                   histogram  ,
                   xvar       ) :
        
        super().__init__ ( particle   ,
                           criterion  ,
                           sample     ,
                           magnet     ,
                           cuts       )
        
        assert isinstance ( histogram , ROOT.TH1 ) and 1 == histogram.GetDimension() , \
            "Invalid histogram type %s" % typename ( histogram )
        
        self.__histogram = histogram
        self.__xvar      = xvar

    def variables ( self ) : return self.__xvar  , 
    def histogram ( self ) : return self.__histogram 
   
# =============================================================================
## @class PARTICLE_2D
#  The actual function to produce 2D efficiency, e.fg. as function of P&eta 
#  @author Vanya BELYAEV Ivan.Belyaev@itep.ru
#  @date   2014-05-10
class PARTICLE_2D(PARTICLE):
    """ The actual function to produce 2D efficiency, e.g. as function of P&eta
    """    
    ## create the object
    def __init__ ( self       ,
                   particle   ,
                   criterion  ,
                   sample     ,
                   magnet     ,
                   cuts       ,
                   histogram  ,
                   xvar       , 
                   yvar       ) :
        
        super().__init__ ( particle   ,
                           criterion  ,
                           sample     ,
                           magnet     ,
                           cuts       )
        
        assert isinstance ( histogram , ROOT.TH1 ) and 2 == histogram.GetDimension() , \
            "Invalid histogram type %s" % typename ( histogram )
        
        self.__histogram = histogram
        self.__xvar      = xvar
        self.__yvar      = yvar
        
    def variables ( self ) : return self.__xvar , self.__yvar 
    def histogram ( self ) : return self.__histogram 
        
# =============================================================================
## @class PARTICLE_3D
#  The actual function to produce 3D efficiency, e.fg. as function of P&,eta& #trk 
#  @author Vanya BELYAEV Ivan.Belyaev@itep.ru
#  @date   2014-05-10
class PARTICLE_3D(PARTICLE):
    """ The actual function to produce 3D efficiency, e.g. as function of P,eta& #trk
    """        
    ## create the object
    def __init__ ( self       ,
                   particle   ,
                   criterion  ,
                   sample     ,
                   magnet     ,
                   cuts       ,
                   histogram  ,
                   xvar       , 
                   yvar       , 
                   zvar       ) :
        
        super().__init__ ( particle   ,
                           criterion  ,
                           sample     ,
                           magnet     ,
                           cuts       )
        
        assert isinstance ( histogram , ROOT.TH1 ) and 3 == histogram.GetDimension() , \
            "Invalid histogram type %s" % typename ( histogram )
        
        self.__histogram = histogram
        self.__xvar      = xvar
        self.__yvar      = yvar
        self.__zvar      = zvar
        
    def variables ( self ) : return self.__xvar  , self.__yvar, self.__zvar 
    def histogram ( self ) : return self.__histogram 


# ===================================================================================================
    
# =============================================================================
if '__main__' == __name__ :
    
    from ostap.utils.docme import docme
    docme ( __name__ , logger = logger )
    
# =============================================================================
##                                                                      The END 
# =============================================================================



"""

from pidcalib2.make_eff_hists import decode_arguments
from pidcalib2.utils          import create_histograms 
from pidcalib2                import pid_data


args = [
    '--sample'      , '20'   ,
    ## '--sample'      , 'Turbo18'   ,
    '--magnet'      , 'up'        ,  
    '--particle'    , 'Pi'        , 
    '--pid-cut'  , 'DLLK > 4'  ,
    ## '--pid-cut'     , "MC15TuneV1_ProbNNp*(1-MC15TuneV1_ProbNNpi)*(1-MC15TuneV1_ProbNNk) < 0.5 & DLLK < 3"  , 
    '--bin-var'    , 'P'         , 
    '--bin-var'    , 'ETA'       , 
    '--bin-var'    ,  'nSPDhits' , 
    '--output-dir' , 'pidcalib_' , 
    '--max-files'  , '10'         ] 


result = decode_arguments ( args )

print ( result )
config = vars ( result )

print ( config ) 

calib_sample = pid_data.get_calibration_sample (
    config [ 'sample'       ] ,
    config [ 'magnet'       ] ,
    config [ 'particle'     ] ,
    config [ 'samples_file' ] ,
    config [ 'max_files'    ] ,
    
)
print ( 'CALIB_SAMPLE' , calib_sample )

tree_paths = pid_data.get_tree_paths (
    config["particle"],
    config["sample"],
    (
        calib_sample["tuple_names"][config["particle"]]
        if (
                "tuple_names" in calib_sample
                and config["particle"] in calib_sample["tuple_names"]
        )
        else None
    )
    )

print ( 'tree_PATHS' , tree_paths  )

mass_fit_variables = pid_data.get_mass_fit_variables(
    config["sample"],
    config["magnet"],
    config["particle"],
    config["samples_file"],
    config["max_files"],
)

print ( 'mass-fit-variables' , mass_fit_variables )


## create_histograms ( config ) 

# If there are hard-coded cuts, the variables must be included in the
# branches to read.
cuts = config["cuts"]
if "cuts" in calib_sample:
    if cuts is None: cuts = []
    cuts += calib_sample["cuts"]
        
sweight_branch = str(
    "probe_sWeight"
    if "sweight_branch" not in calib_sample
    else calib_sample["sweight_branch"]
)
sweight_branch_to_pass = None if "sweight_dir" in calib_sample else sweight_branch

if "probe_prefix" in calib_sample:
    particle_label = str(calib_sample["probe_prefix"])
else:
    particle_label = None
    
branch_names = pid_data.get_relevant_branch_names(
    config["pid_cuts"],
    config["bin_vars"],
    cuts,
    sweight_branch_to_pass,
    particle_label,
)
branch_names.update({
    mass_fit_variable: mass_fit_variable for mass_fit_variable in mass_fit_variables
})



print ( 'CUT                  ' , cuts )
print ( 'SWEIGHT_BRANCH       ' , sweight_branch  )
print ( 'SWEIGHT_BRANCH_PASS  ' , sweight_branch_to_pass   )
print ( 'PARTICLE_LABEL       ' , particle_label )
print ( 'BRANCH_NAMES         ' , branch_names ) 

import ostap.trees.trees
import ROOT


for tree in tree_paths :
    ch = ROOT.TChain ( tree )
    for f in calib_sample['files'] :
        ch.Add ( f )
    print ( 'TREE' , tree , len ( ch  ) ) 
            

h1 = ROOT.TH1D ( 'h1' , '' , 20 , 0 , 2 )


p = PARTICLE_1D ( 'Pi'                              ,
                  'probe_MC15TuneV1_ProbNNpi>0.5'   ,
                  'Turbo18'                         ,
                  'up'                              ,
                  ''                                ,
                  histogram = h1                    ,
                  xvar      = 'log10(probe_P/1000)' ) 

from ostap.utils.timing import timing 
from ostap.trees.utils  import Chain


tree_name = p._PARTICLE__tree_paths[0]
files     = p._PARTICLE__data['files']

with timing ( 'CREATE' , logger = logger ) :
    ch = Chain ( name = tree_name , files = files )

with timing ( 'SPLIT' , logger = logger ) :
    tt = [ t for t in ch.split() ]

import pickle 
with timing ( 'PICKLE' , logger = logger ) :
    tt = [ pickle.loads ( pickle.dumps ( t ) ) for t in tt ]

    
"""




