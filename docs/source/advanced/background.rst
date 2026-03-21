Dataflow History
================

.. _history:

In 1975, Jack Dennis and David Misunas at MIT wrote a landmark paper entitled `A
Preliminary Architecture for a Basic Data-Flow Processor
<https://dl.acm.org/doi/abs/10.1145/642089.642111>`_ (see also `How It All Began
- Computation Structures Group - MIT
<http://csg.csail.mit.edu/Dataflow/talks/DennisTalk.pdf>`_).  In it, they laid
out a profound and novel concept for organizing computation.  Unlike the von
Neumann architecture that operates by fetching data (e.g. from registers) when a
specific instruction is selected by a program counter for execution, Dennis and
Misunas inverted this model by *fetching an instruction and executing it when
the data became available*.  More than wordplay, this inversion changed the
fundamentally-sequential von Neumann model to a fundamentally-parallel dataflow
model.  To this writer's awareness, this was the single most significant advance
in all of computing in the name of parallel computation.

Machine-level representations of programs in a dataflow processor are *directed,
acyclic graphs*.  Nodes in the graph represent primitive instructions (e.g.,
``add``, ``multiply``).  Each such node has an output *arc* emanating from it.
The arc represents the logical data-flow connection (hence, the name) from that
instruction to any subsequent instructions that depend on it.  Execution of such
a graph is easy to visualize.  We imagine that these data values flowing across
the arcs are markers, called ``tokens`` bearing the *value* of the instruction
that produced them.  Imagine an ``add`` instruction with two incoming arcs.  If
there were a token on each input arc, representing the values to be added, we
would say that the instruction is *enabled* because we know all we need to know
to proceed.  Any such enabled instruction can be selected for execution -- we
speak of ``firing`` the instruction.  This process gobbles up the input tokens,
computes the sum of the values on these tokens, and produces a new token on the
output arc that bears the value of this sum.

Now imagine a large graph made up of thousands of such primitive nodes, all
interconnected with arcs.  We drop input tokens into the input arcs (the ones
that are not, themselves, connected to the output of any nodes in the graph).
These tokens percolate down through the graph like marbles, computing as they
go.  The brilliant bit is this: at any time, *all* of the instructions that are
simultaneously enabled can be fired with no regard whatsoever to the temporal
ordering between them, **up to and including computing all of them in
parallel**.  This means that the graph could, potentially, be cut up into many
separate parts and distributed to various interconnected processors.  Each
processor could then focus its attention on executing all the enabled
instructions in its part of the graph with no regard for what any other
processor might be doing.  The only way one part of the program can influence
another is by sending some tokens to it.  All data dependencies are explicit,
all communication is implicit, and all primitive nodes employ synchronization
barriers to await inputs.

This property of dataflow computation allows us to separate the question of
"what answer do I get from this graph given these inputs?" from the question of
"how should I cut up this graph to optimize its performance, given that I have a
number of processing nodes?"  We can appreciate this kind of elegance purely
from a theoretical perspective.  But as we shall see, it also has great
practical significance.

Matching Tokens
---------------

Arvind and Plouffe generalized the Dennis & Misunas model by recognizing that a
single graph representing a compiled program will need to be invoked many times
concurrently in the same way that when you call the FFT library and I call it,
there's no confusion about whose-data-is-whose. But if all we have are marbles
flowing on the graph, how do we keep yours separated from mine?  The solution
came with the notion of assigning each token some additional information in the
form of a ``tag`` to represent these different contexts (in modern processors,
this is handled using process-specific program counters and stack frames).  One
can visualize this as having differently-colored sets of marbles on the graph.
You have red ones, and I have blue ones.  Now, the rule for firing a node
becomes slightly more interesting -- we only match tokens based on like-color
(red with red, for instance), and we further impose a rule that says color
begets like color (red tokens can only give birth to more red tokens).  This
scheme is conceptually elegant, but there are complexities in making sure one
does not run out of colors, that new colors can be created in a distributed way
without creating a potential conflict, and so on. These problems have all been
solved, and the body of work is known as the MIT Tagged-Token Dataflow (TTDF)
system.  The interested reader is referred to the `literature
<https://doi.org/10.1109/12.48862>`_.  But suffice it to say that this tag
calculus is sufficient for preventing unwanted conflict, and execution rules
that respect tags will result in graphs that are self-cleaning ('no token left
behind' -- important for storage management).

Scheduling Quanta
-----------------

A further improvement was made by Iannucci who recognized that it is often
beneficial in terms of performance to group together certain nodes in the graph
and to NOT use dataflow-style token passing between them but rather to employ
temporary storage (called *evaporating registers*) for holding token values
between instructions in such groups.  This approach was called Hybrid von
Neumann-Dataflow and introduced the idea that these groups of instructions could
be efficiently computed using hardware threading.  The groups of instructions
retained the local-only data reference properties of pure dataflow, and by
proper grouping would have the property that once started, each could always be
run to completion with no external synchronization needed.  These units are
called *scheduling quanta* ``SQ`` because they are the smallest
independently-schedulable unit of work and that they are in essence indivisible.
At the boundary, an ``SQ`` behaves just like a primitive ``add`` or ``multiply``
in a dataflow graph.  But internally it can be much more complex than a single
primitive instruction which can be used to improve efficiency over pure
dataflow.  We will return to this concept shortly.


Relevance to the IoT
--------------------

In the Computation and Communication Structures Group (`CCSG
<http://ccsg.ece.cmu.edu>`_) at CMU, we see the Internet of Things as a
large-scale, connected embedded system.  Each node computes some part of a
larger system-wide program (or programs) and sends sensed values and computed
values off to other nodes in the system.  When considering how to develop a
programming model for such IoT systems, we recognized that many properties of
dataflow graphs would be beneficial.  In a smart city, for instance, we might
imagine writing an application to perform analysis of traffic flows by fusing
readings from a variety of sensors to form a real-time model of what's happening
on city streets and then using this model to optimize traffic signals, to warn
of impending collisions and the like.  Our colleagues at Arizona State
University took this one step further -- using such a model to control vehicle
speeds to enable collision-free cross-flows of vehicles through intersections
*without the need for traffic signals*.  Casting this *app* as a graph and then
mapping the graph to the available sensing, processing, and actuating units
would make it relatively portable.  Dataflow-style token-passing would form the
core communications mechanism.  Instruction processing would be based on the
arrival of data tokens.  Static and dynamic partitioning, or mapping, algorithms
would enable optimization for speed, power, or other criteria and would not
impact program correctness in the process.

