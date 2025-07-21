# pidcalib


The efficient processing of LHCb/PidCalib PID calibration data  using [ostap] project 
Actually it is just a tiny wrapper over
    [pidgen2] package by Antom Poluektov @apoluekt and [pidlicab2] package by Daniel Chervenkov @dcervenkov  
    
## Dependencies

- _mandatory_: [ostap] 
- _mandatory_: [pidgen2] 
- _mandatory_: [pidcalib2]
    

## PidGen
```
  ## input MC data to be resampled 

  >>>  data_2016u = ROOT.TChain ( ... )
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
    
```

    
## PidCorr 
```
        
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

```

## PidCalib
    
```
    
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
    
```

   
[ostap]: https://github.com/OstapHEP/ostap
[pidgen2]: https://pypi.org/project/pidgen2/
[pidcalib2]: https://pypi.org/project/pidcalib2/