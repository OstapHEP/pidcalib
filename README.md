# pidcalib : The efficient processing of LHCb/PidCalib PIC calibration data  using [ostap] project 


## Dependencies

- _mandatory_: [ostap] 

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
pid_calib2.py -h
```

- If calibration samples to be taken from the Grid, one needs to ensure that scripts in the directory [scripts] are in the path, e.g. 
```
PATH=<...>/scripts:$PATH pid_calib2.py ... 
```
Also for this mode one needs 
 - access to LHCbDirac at `cernvmfs`, see the content of `dirac-command` script in [scripts] directory  
 - valid Grid proxy 

- In case `/eos/lhcb/grid` is accessible directly, no Grid proxy and machinery is requred. 
In this mode, a powerfull [pathos]-based parallelization is available, activated with `-z/--parallel` keys
```
pid_calib2.py --parallel 
```

- If `/eos/lhcb/grid` is accessible directly, but input data are requested from the Grid, the optional conversion using the comman line flag `--useeos` is possible. It opens a way for the parallel processing.

### Run I processing 
 


[ostap]: https://github.com/OstapHEP/ostap
[pidcalib]: https://github.com/OstapHEP/pidcalib
[examples]: https://github.com/OstapHEP/pidcalid/examples
[scripts]: https://github.com/OstapHEP/pidcalid/scripts
[pathos]: https://github.com/uqfoundation/pathos