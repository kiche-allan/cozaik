System Attributes
=================

TTPython, then, is a software-development system for loosely-timed *smart-whatever* systems with the following attributes:

* It is a **programming language** in which core IoT timing concepts are manifest, enabling a variety of implicit and explicit tools for dealing with fuzzy time in IoT systems.

* It is a **compiler** that translates TTPython programs into dataflow graphs in which the nodes in the graph are annotated with programmer-derived timing annotations.  

* It is a **graph interpreter** that enables programmers to analyze the logical correctness of TTPython programs at runtime, up to and including streaming sources of real or simulated data while keeping time consistently per program requirements such that periodicities or deadlines are respected across a set of streaming sources, processors, and sinks such that multiple streams may be synchronized according to a shared timeline (i.e., a synchronized *clock*). 

* It is a **distributed runtime environment** that executes dataflow graphs on a set of physical or simulated devices (referred to as *ensembles* of computing, storage, time-keeping, sensing, and actuating hardware elements), which can be plugged into other simulation engines such as the Simulator of Urban Mobility (`SUMO <https://www.eclipse.org/sumo/>`_), working as a front-end to enable programming of large, heterogeneous systems. This front-end enables studies of ease-of-programming (or not) by programmers who do not specialize in timed-dataflow, low-power, connected embedded system software design.


