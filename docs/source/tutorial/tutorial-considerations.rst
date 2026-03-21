Details to Consider
====================

.. _tutorial-considerations:


This page is dedicated to providing a list of 'considerations' that the user should be cognizant of when writing and running their TTPython program. We'll split that into two respective sections.


Programming Considerations
--------------------------


*  If you intend to use a library within an SQ, you must import it *within* the ``SQify`` -d function. This is because each SQ runs in its own isolated Python namespace.
*  Do not attempt to create/use shared variables between SQs; there is no shared memory between SQs. Our architecture relies on local state and message passing for sake of isolation and consistency.
*  The root clock will suffice for almost all timing behaviors, although child clocks are useful for limiting the precision of timestamps for the sake of readability or clarity. By default, the Root clock ticks once per microsecond.
*  Functions can take keyword arguments as parameters, but these must be constant valued. Expressions or function calls will be misinterpreted by the compiler as SQs themselves. Meta parameters are the only potential exceptions since the compiler can treat them as a special case; these meta parameters' names begin with "TT".
*  To directly work with tokens, their time-intervals and values, the keyword argument "TTExecuteOnFullToken=True" can be provided in the function definition of an SQ. Note that to create the new output token, Token and Time must be imported. There are examples of this in the :ref:`primitive instructions<instructions>`.


Runtime Considerations
----------------------

*  Each ensemble will have a name and address; this name should be globally unique (within the set of devices that will connect to run this program), and the address (nominally, an IP address) should be accessible from any other ensemble in the network.
*  The runtime manager should let ensembles connect before it attempts to map SQs from the graph to ensembles. It is most convenient to have this runtime manager on an ensemble that allows direct user input. A good example is in tests/multidev-test-rtm.py.
*  Output arcs of the graph are the outputs of SQs that have no destination. The resulting tokens on these arcs are always sent to the runtime manager, which logs the token's string representation along with the source SQ and ensemble. This is written to 'output.log' in the directory that the runtime manager is executed from.
*  Values that go into tokens must be serializable. We use the 'pickle' library to serialize, and it will raise an exception for values that cannot be converted to a byte-array representation.
*  When synchronizing input tokens, if there are multiple candidate overlaps in time-intervals between the newest token and the stored token, the largest overlap will be chosen (and correspondingly, the tokens with the largest intersection with that total overlap). The user may run into this problem if the stream's periodicity is less than half the tokens' data validity interval (time interval) within that stream. Setting the phase of the streaming to be consistent is one way to avoid this issue
*  When a stream is being generated, the tokens will have some data validity interval, which is used to set the width of the time interval. The center of that time interval is based when the SQ executed, not when it was *supposed* to execute (that center timestamp is calculated at the midpoint between when the SQ started and finished execution*).
