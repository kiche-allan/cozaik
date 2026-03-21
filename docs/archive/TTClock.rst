TTClock
=======

.. comment: This has been archived because TTPython's 1.0 implementation does not practice what this page preaches. We have not actually implemented this generalization on clock synchronization that allows us to sync within some programmer defined tolerance -- this requires lower level implementations that interact directly with network interfaces and time-keeping hardware. Python is probably the wrong environment to do that in, and the solutions are unlikely to be particularly portable. The Timeline abstraction Anware et al. attempts to implement this within Linux. TTClocks in TTPython 1.0 uses them as a specification for timelines, but derives the current time from the system clock  (time.time()) -- the clock sync error is not represented. In this way, TTClocks are effectively just a lossy conversion from the root clock (which is assumed to be this system clock), to the point where there is little to no benefit of using the whole clock tree (or really any derived clock).

In TTPython, all data values are represented by tokens, and all tokens are timestamped (see :ref:`TTTime`).  Timestamps derive from clocks.  But recognizing that in a large, distributed system that referencing a single **golden** clock is near-impossible due to latency and power considerations, we design our system with the notion that individual devices like sensors have their own clocks that need to be periodically *synchronized* to the golden, or root, clock.  In the general case, we imagine the clocks in a large system to be organized in a *tree* in which there is a root clock and a set of clocks that are sychronized to it.  In like manner, there may be further-subordinate clocks that are synchronized to clocks synchronized to the root, and so on.  In our system, we assume this synchronization relationship to be one-to-one (other than root, which does not synchronize to anything else, each clock synchronizes to exactly one other clock).  We can understand from the clock tree and its properties how to communicate a time-label that is based on one clock in the tree to a time-label that is based on another clock in the tree.

Definitions
-----------

:root clock:  (or just **root**) The top clock of the clock tree.  In the classic clock tree literature, e.g. in telecommunications, this is also commonly called **stratum 0**. An example would be a UTC primary clock
:derived clock: A clock below **stratum 0** is commonly called a *derived clock* because it derives its synchronization from another clock.  The root clock, of course, does not have another clock from which it is derived.
:parent clock: For any clock other than **root**, the clock from which it immediately derives its synchronization

Every derived clock in the tree is synchronized by some mechanism to its parent clock.  Every clock in the tree counts in integer *ticks*.  This is an important concept.  In our system, we do not have a way to express a point in time that is infinitessimally narrow.  Instead, we mark time in *intervals*.  These may be quite small, but nevertheless, they span some finite segment of the real time line.  This also has implications for the underlying clock hardware.  Just as a real clock counts in ticks (even mechanisms like the old escape wheel), so we also make this fundamental assumption.  It is ultimately a choice for the programmer to define the smallest time interval (including time offsets) in a TTPython program because the underlying hardware can only count in discrete steps.  As will become clear, we must assume that the smallest interval of time plays itself out as the tick rate of the clock at the top of the tree.  Every other tick-rate or time offset can only be expressed in terms of integer ticks of the top clock.

When we say that one clock is synchronized to another, we mean that the hardware counters of the two clocks share some basic relationship that is expressed in terms of integer properties.  One such property is the relative tick rate.  The root clock ticks the fastest in our system.  Clocks synchronized to it must tick either at the same rate or at a rate determined by an **integer divisor** called the *period* -- meaning that it ticks **more slowly** than the clock to which it is synchronized in a ratio of *period* ticks of the parent to one tick of the derived clock.

.. note:: There are some possible exceptions to this rule in that certain hardware structures (e.g., phase-lock loops) allow for a derived clock to in fact tick at a rate higher than its parent and by this means, it is possible to identify time intervals that are shorter than those expressed by the root.  But this has limited applicability, as we shall see, in communicating those times to other parts of the program governed by the root clock and its ability to mark time intervals only in integer numbers of root ticks.

Time values for a given clock form an integer sequence that is monotonic increasing.  This sequence can be thought of as a `namespace <https://en.wikipedia.org/wiki/Namespace>`_.  We will return to this concept when we discuss **time-based dataflow**.  For now, we only need to state that when a derived clock is established, we not only can set the relative tick rate (*period*), we als can establish the point on its parent's time line at which the **derived clock** is at *t=0*.  We call this the *epoch*.

:period:  for a given derived clock, a positive integer that expresses the number of ticks of the parent clock per tick of the derived clock
:epoch:   for a given derived clock, an integer that expresses the smallest tick of the parent clock corresponding to the derived clock's *t=0*
:ancestor clock: or a given derived clock, any of the clocks along the tree starting from and including the parent clock to the root clock.

.. note:: For any two clocks in the tree, there will always be a common ancestor clock (possibly the root clock).

The TTClock class
-----------------

The ``TTClock`` class implements the above.  Every TTPython program needs at least one clock (the root clock).  When a TTPython program is designed to allow parts to be mapped to more than one computing device (**ensemble**), the programmer will need to define additional ``TTClocks`` and the tree relationship between them.

.. automodule:: ticktalkpython.Clock
   :members:
