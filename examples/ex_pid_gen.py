

import ostap.core.pyrouts
import ROOT

from pidcalib.pidgen import PidGen, runPidGen, run_pid_gen 

chain = ROOT.TChain ( 'tMC' )
chain.Add ( 'mc_tree_2011_u_test_pidgen0.root' )
chain.Add ( 'mc_tree_2011_u_test_pidgen1.root' )
chain.Add ( 'mc_tree_2011_u_test_pidgen2.root' )
chain.Add ( 'mc_tree_2011_u_test_pidgen3.root' )
chain.Add ( 'mc_tree_2011_u_test_pidgen4.root' )
chain.Add ( 'mc_tree_2011_u_test_pidgen5.root' )
chain.Add ( 'mc_tree_2011_u_test_pidgen6.root' )
chain.Add ( 'mc_tree_2011_u_test_pidgen7.root' )
chain.Add ( 'mc_tree_2011_u_test_pidgen8.root' )
chain.Add ( 'mc_tree_2011_u_test_pidgen9.root' )
chain.Add ( 'mc_tree_2011_u_test_pidgen10.root' )


pidgen = PidGen ( 'log(pt_pion0_pid*1000)'  ,
                  'eta_pion[0]'             ,
                  'log(corr_nTracks_pid)'   ,
                  ## config = 'pi_V3ProbNNpi' ,
                  config  =  'pi_MC15TuneV1_ProbNNpi_Brunel_Mod2',                  
                  dataset = 'MagUp_2017'    ,
                  silent  = True            )

## import ostap.parallel.parallel_add_branch
## runPidGen ( chain , pidgen = pidgen , newpid = 'NewPID44' , silent = False , variants = True )

run_pid_gen ( chain , pidgen = pidgen , newpid = 'NewPID65' , silent = False , variants = True , parallel = True )






