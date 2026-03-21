*******
API
*******

.. toctree::
   :maxdepth: 2
   :hidden:

   classes/TTArc
   classes/TTClock
   classes/TTCompiler
   classes/TTCompilerRules
   classes/TTComponent
   classes/TTDeadline
   classes/TTEnsemble
   classes/TTExecuteProcess
   classes/TTGraph
   classes/TTInputTokenProcess
   classes/TTIPC
   classes/TTMapper
   classes/TTNetwork
   classes/TTNetworkManager
   classes/TTPlanB
   classes/TTRuntimeManager
   classes/TTSQ
   classes/TTSQExecute
   classes/TTSQSync
   classes/TTTag
   classes/TTTime
   classes/TTToken

This section of the reference manual provides the APIs used by a set of core classes used within the compiler and runtime environment. This runtime environment is where the TTPython program is interpreted as a timed dataflow graph, which the compiler has created from a ``GRAPHify``-ed function. The runtime can be on simulated ensembles (using the simpy simulation environment) or a set of physical ensembles. The differences between these environments is minimized to make most mechanics as reusable as possible; the main differences are in the ensemble configuration, the network interface(s), and how the passage of time is treated.