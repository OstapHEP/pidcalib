# pidcalib : The efficient processing of LHCb/PidCalib PID calibration data  using [ostap] project 
Actually it is just a tiny wrapper over [pidgen2] package by Antom Polurktov
    
## Dependencies

- _mandatory_: [ostap] 
- _mandatory_: [pidgen2] 


Most likely all `pidcalib` machinery is obsolete now (2025)
but the `pidgen2` functionality is just updated


## `pidgen1`

### PidGen
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
### PidCorr 
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


    
## Features

- The project allows to get 1D, 2D or 3D efficiency historgams for particle identification using the calibraton samples 
- Calibration samples can be accessed either from CERN eos or via GRID 
  - the first way requires read access to `/eos/lhcb/grid` partition 
  - the second way require to have valid Grid proxy and access to LHCbDirac machinery via cernvmfs 
  - both conditions are satisfied on CERN   machines, `lxplus.cern.ch` in particular 
- The calbration samples can be processed in parallel using [pathos]-based parallelisation
- The output histograms and useful processing information are placed in the outptu database, default is `PIDCALIB.db`   

## Usage

### Run II processing 

To use this function a helper script needs to be created, see example in [examples] directory.
The script to be execute from command-line 
```
pid_calib2.py 
```
All command-line arguments can be inspected using `-h` flag
```
ex_pidcalib_run2.py -h
```

- If calibration samples to be taken from the Grid, one needs to ensure that scripts in the directory [scripts] are in the path, e.g. 
```
PATH=$PATH:<...>/scripts ex_pidcalib_run2.py ... 
```
Also for this mode one needs 
 - access to LHCbDirac at `cernvmfs`, see the content of `dirac-command` script in [scripts] directory  
 - valid Grid proxy 

- In case `/eos/lhcb/grid` is accessible directly, no Grid proxy and machinery is requred. 
In this mode, a powerfull [pathos]-based parallelization is available, activated with `-z/--parallel` keys
```
ex_pidcalib_run2.py --parallel 
```

- If `/eos/lhcb/grid` is accessible directly, but input data are requested from the Grid, the optional conversion using the comman line flag `--useeos` is possible. It opens a way for the parallel processing.

### Run I processing 
 

```
ex_pidcalib_run11.py  P -s 20 -p MagUp -c 'P_hasRich==1'
```

## PidGen processing 

It is important to have an environment consuisten with Urania-environment
e.g. choose Urania/v10r1 

```
lb-set-platform x86_64_v2-centos7-gcc11-opt
... build ostap with LCG_101 ... 

```


[ostap]: https://github.com/OstapHEP/ostap
[pidgen2]: https://pypi.org/project/pidgen2/

[pidcalib]: https://github.com/OstapHEP/pidcalib
[examples]: https://github.com/OstapHEP/pidcalid/examples
[scripts]: https://github.com/OstapHEP/pidcalid/scripts
[pathos]: https://github.com/uqfoundation/pathos