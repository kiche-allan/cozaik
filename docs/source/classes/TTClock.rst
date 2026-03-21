TTClock
=======

In TTPython, all data values are represented by tokens, and all tokens are timestamped (see :ref:`TTTime`).  Timestamps derive from clocks. TTClocks represent timelines that are relatve to another timeline or *clock domain*. Each TTClock has a parent clock from which it derives its notion of time. Of these TTClocks, one must have a direct relation to real-time (or some time-keeping hardware); this is the 'root' clock, which has no parent. In this way, TTClocks within a TTPython program form a tree, which is conceptually similar to 'Stratum' in clock synchronization frameworks like the Network Time Protocol.

Currently, the clock tree is used as a specification for these timelines, but was originally conceived as a generalization of clock synchronization trees in which derived clocks operated at lower precision than their parent. This would define how finely clocks should be synchronized to meet the application requirements. Constructing trees in this way has a strong relation to the mapping of the program and how well ensembles can synchronize their clocks with each other. For now, we rely on the underlying system to synchronize the root clock to Universal Coordinated Time to the best of its abilities (this is generally on the order of 5 ms).

Every clock in the tree counts in integer *ticks*.  This is an important concept.  In our system, we do not have a way to express a point in time that is infinitessimally narrow.  Instead, we mark time in *intervals*.  These may be quite small, but nevertheless, they span some finite segment of the real time line.  This also has implications for the underlying clock hardware.  Just as a real clock counts in ticks (even mechanisms like the old escape wheel), we also make this fundamental assumption.  It is ultimately a choice for the programmer to define the smallest time interval (including time offsets) in a TTPython program because the underlying hardware can only count in discrete steps.

Time values for a given clock form an integer sequence that is monotonic increasing.  This sequence can be thought of as a `namespace <https://en.wikipedia.org/wiki/Namespace>`_.  For now, we only need to state that when a derived clock is established, we not only can set the relative tick rate (*period*), we also establish the point on its parent's time line at which the **derived clock** is at *t=0*.  We call this the *epoch*.

:period:  for a given derived clock, a positive integer that expresses the number of ticks of the parent clock per tick of the derived clock
:epoch:   for a given derived clock, an integer that expresses the smallest tick of the parent clock corresponding to the derived clock's *t=0*
:ancestor clock: or a given derived clock, any of the clocks along the tree starting from and including the parent clock to the root clock.

.. note:: For any two clocks in the tree, there will always be a common ancestor clock (possibly the root clock).

The TTClock class
-----------------

The ``TTClock`` class implements the above.  Every TTPython program needs at least one clock (the root clock).

.. automodule:: ticktalkpython.Clock
   :members:
