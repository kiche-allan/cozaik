Clocks and Time
================

.. _clocks-and-time:

This page describes some of the finer points with Clocks and Time, both in their
specification and semantics within during compile and run time, respectively. We
also point out a few nuances with the clock model.


Clock Trees
------------

In TTPython, clocks are defined to always have some parent clock, save for the
"Root" clock, which is named for its place in the clock tree. From a naive
perspective, the clocks in this tree are similar to the concept of "Stratum" in
the Network Time Protocol, in which there are levels within a synchronization
tree that define the distance of a clock from trusted time authority (like
NIST). Synchronization (in)accuracy can compound with distance from the root of
the tree in these Stratum as well as our notion of clock trees (which are
notably simpler at this time, given NTP's sophisticated use of clock selection,
peer synchronization, and a variety of clock synchronization mechanisms designed
to improve stability and accuracy over the long term).

We have a choice whether to use the clock tree as an alternative to NTP, in
which the clock tree embeds the necessary timing precision (and thus, a lower
bound on synchronization precision) into the definition of each clock, or to
simply use the clock tree for making specifications of timescales/timelines. At
this time, we are instead distributing all clocks in the tree to all ensembles,
and assuming the root clock of this tree is a proxy for Universal Coordinated
Time (UTC) to whatever precision the Ensemble can achieve (typically 5-10ms
using NTP with an arbitrary link to the clock synchronization sources). In this
way, the clocks in the tree effectively provide an alternative way for the
programmer to think about time, *but they are free to use the Root clock for
everything if this is most intuitive*.

.. note:: The `Timelines Abstraction
    <https://users.ece.cmu.edu/~agr/resources/publications/RTSS16-timeline.pdf>`_
    solves this exact problem, but did not achieve widespread use, likely due to
    a lack of applications (or pereception thereof) that would obviously
    benefit.

Reading Clocks at Runtime
--------------------------

At runtime, reading the clock means making a system call to read a clock
provided by hardware or the operating system. An arbitrary ``TTClock`` can
receive a timestamp (an integer) by calling a function 'now' (e.g.,
``current_timestamp = clock.now()``), which traverses the tree until it finds a
clock with a function that actually reads from a synchronized clock. By default,
only the root clock has this privilege to directly read a synchronized clockk,
and a function to read this synchronized clock is provided whenever the root
clock is instantiated on the Ensemble. Derived/child clocks can read time, but
do so by traversing the tree to the root. Reading a clock always returns the
current time in integer ticks within the clock's domain (in the root clock,
default to 1,000,000 ticks per second -> a microsecond per tick). The semantics
are identical in the physical and simulated runtimes.

Imposing Delays and Deadlines at Runtime
-----------------------------------------

SQs are not intended to encode delays, timeouts, or deadlines directly within
the ``SQify``-ed function. This is meant to happen in the surrounding
architecture, primarily to avoid inconsistent or dangerous behavior, such as
halting a process that services many SQs (``TTExecuteProcess``) just so one SQ
can do as it pleases.

TTPython employs two Real-Time Operating System primitives, 'wait' and
'wait-until', which will wait a set duration or wait until a specific time (with
respect to a (synchronized) clock). This is generally used for delaying tokens'
arrival to the synchronization stage of SQs, but can be used for a variety of
purposes. These two methods will convert a time to wait into the same timescales
that the underlying hardware clocks use. It then attaches a callback and value
so that when the duration expires, the callback function will be called on the
provided value. This delay occurs asynchronously to the rest of the processes to
avoid blocking or deadlock.
