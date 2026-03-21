Compilation
===========

.. _compile:

The TTPython Language compiles into a Timed Dataflow graph, composed of
:ref:`Scheduling Quanta<sq>` and arcs connecting them. The compilation builds
this graph by turning expressions (like multiplication or negation) and function
calls into SQs and variable names into arcs.

There are several more advanced topics and considerations within the compiler
that programmers and TTPython developers need be aware of.

SQ and Firing Rule Creation
----------------------------

SQs are created during compilation from expressions and function calls. Basic
mathematical expressions, like multiply, divide, add, etc. are overloaded to be
replaced by SQs that perform this operation. These are some of the
:ref:`primitive instructions<instructions>`, which can also be called directly
as MULT, DIV, ADD, and so on. These purely computation SQs are very simple and
require no additional input from the user, as there are no mechanisms outside of
the ``SQify'd`` function that need configuration.

SQs will automatically be given an output arc and a set of input arcs based on
variables, output assignment, and/or intermediate expressions. Note that all SQs
will have an input arc, even if there is not one in the original program.
Constant values (e.g. ``x = 3 * 5`` where 3 and 5 are constants) are themselves
SQs that use an input trigger at the initiation of the program to produce the
constant value.

The input arcs are also used to analyze the upstream SQs to see if their output
behavior will impact how this downstream SQ should behave, primarily with
regards to firing rules and token storage. SQs may follow a :ref:`type or
pattern<sqpattern>`, like Trigger In, N Out -- in this scenario, consider an SQ
that wishes to apply a simple conversion or math operation (like Celsius to
Fahrenheit) on a stream of values. If the transformation is encoded entirely
into an SQ, then this is trivial; one in, one out. However, if the conversion is
applied within the TTPython program using operations as SQs (*e.g.*, ``F = 9/5*C
+ 32``), then the constant values are produced as tokens, but rather than
generating them for every iteration of a streaming arc 'C', those tokens could
be reused. They would 'stick' to the input ports of these streaming operation.
This is an example of graph analysis, where we determine that the multiplication
of 9/5 with C should reuse the 9/5 value but not C because we are combining a
stream with a non-stream, generating another stream. In this example, ``9/5``
would be its own SQ, producing a token with value ``1.8``, and that result would
be send along an input arc for ``x*C``, where x=1.8. Graph analysis will tell us
that the port receiving from ``x`` should be sticky -- the token will not be
removed between invocations for the SQ computing ``x*C``, although tokens
corresponding to ``C`` *will* be removed. Graph analysis has informed how we
treat different these inputs based on their sources.

In some cases, such as stream generation with the decorator ``STREAMify``, the
user may need to provide more information or rely on the compiler to make some
assumptions. For instance, a ``STREAMify`` node is always assumed to use a
particular firing rule, ``TimedRetrigger`` to produce a periodic stream of
sampled inputs. However, parameters like the frequency, phase, data validity (of
sampled inputs), and clock domain must either be provided or assumed. Generally,
we prefer this information to be provided, which is a key topic of the next
subsection.

In addition to the graph structure and decorator, we may determine a firing rule
by a top-level analysis of the users' code. For instance, we apply the
'SequentialRetrigger' token to enforce chronological stream processing whenever
their SQ is noted to receive input from streaming SQs and use internal state via
the ``global sq_state`` mechanism as explained in the :ref:`corresponding
tutorial<tutorial-streamstateful>`.

Arguments, Keyword Arguments, and TTPython Meta-parameters
------------------------------------------------------------

.. _args-and-params:

As we've described throughout the tutorials and documentation, arcs in the graph
are generally created to represent variable names. This explicitly applies to
positional arguments, in which the argument's position within the function call
defines which value the input will be used.

As a quick example,

.. code-block:: Python3

    def foo(a, b):
        return a-b

    c = 2
    d = 3
    foo(c, d)
    foo(d, c)

Funtion 'foo' takes two inputs, and the value for 'a' is filled in by 'c' in the
first call and 'd' in the second; the reverse for variable 'b'. This is how
token-carrying arcs are used, always and forever. No exceptions. Arcs represent
positional inputs in function calls, and never use defaults.

In spite of this, keyword arguments can be used in TTPython, but are not without
restrictions because there are most certainly **not** arcs. To be clear about
what keyword arguments (or 'kwargs') are, see the following example:

.. code-block:: Python3

    def foo(a, b, invert=True):
        if invert: return a-b
        else: return b-a

    c = 2
    d = 3
    foo(c, d, invert=False)
    foo(d, c, invert=True)

This time, we've changed the function to accept an optional argument.
Technically, what's shown is an argument with a default, and it *could* be
satisfied in ordinary Python 3 with including 'invert' as a third argument
without naming it. When the argument is named and given a value in the function
call, that argument name is a keyword. Within the TTPython compiler, we consider
default and keyword arguments effectivelyidentical, such that arguments with
defaults are referred to with keywords. Keyword arguments in TTPython are *not*
arcs, whereas arguments without defaults are positional-only arguments, and are
*always* arcs.

We use keyword arguments with defaults to parameterize SQs at compile time. If
the programmer defines a keyword input like 'invert' in their SQify'd function
defintion, the runtime will ensure that default is provided at runtime. However,
it can also be replaced during the compilation phase. Let's see a quick example:

.. code-block:: Python3

    @SQify
    def foo(a, b, invert=True):
        if invert: return a-b
        else: return b-a

    @GRAPHify
    @def graph(trigger):
        c = 2
        d = 3
        return foo(c, d, invert=False)

The updated default for 'invert' is respected at run time, but the value could
not have been 'trigger', 'c', or 'd' because these **default arguments are
resolved at compile time** by comparing the function call with the function
definition. In general, these values must be constant valued, like numbers,
string literals, or True/False booleans. This mechanism is provided as a means
of parametric input compared to streaming input (a strategy used within the
`Ptolemy Project <https://ptolemy.berkeley.edu/>`_ as well).


Additionally, keyword arguments are used for parameterizing TTPython
functionalities best suited for singular SQs as opposed to groupings (which
typically use the 'with' constructions with Python; see our :ref:`syntax on
'with' for more info<syntax-with>`). These TTPython *meta-parameters* always
begin with "TT", such as "TTPeriod" or "TTPhase" for describing the periodicity
and phase of a stream-generation SQ. An example is shown below:

.. code-block:: Python3

    @STREAMify
    def temperature_sensor_F(trigger):
        import temp_sensor
        return temp_sensor.read_temperature()

    @GRAPHify
    def temp_in_Celsius(trigger):
        with TTClock('root') as ROOT_CLOCK:
            return (9/5) * temperature_sensor_F(trigger, TTClock=ROOT_CLOCK, TTPeriod=1000000, TTPhase=5000000) + 32

This program would parameterize the TimedRetrigger firing rule, which we know to
use given the 'STREAMify' function decorator. The keyword args tell us to use
the clock domain of the ROOT_CLOCK, to produce a temperature sample every
1,000,000 ticks (by default, the root has 1µs length ticks, so once per second),
to and produce samples 500,000 ticks (500ms after top of the second) after the
start of the period so that every sampling will occur at the 500th millisecond
of each second w.r.t. wall-clock time. The programmer used these keyword
arguments beginning with "TT" to determine this behavior. Note that these
TTPython meta-parameter keyword arguments do not always follow the strict
*not-an-arc* requirement, since the compiler specifically knows how to treat
these particular kwargs, such as the ``TTClock`` kwarg.

.. comment: Last part really comes across as 'do what I say, not as I do'

One particularly powerful meta-parameter is worth mentioning here, the
``TTExecuteOnFullToken`` keyword argument. Seen also in several of the
:ref:`primitive instructions<instructions>`, this keyword can be provided as
part of the function definition to give the programmer full access to the set of
tokens provided when the SQ is executed, rather than just the values. This
allows them to read and create the ``TTTime`` for the input and output tokens,
respectively, which is helpful for more complex timing operations and signal
processing. The programmer should use the primitive instructions as templates to
ensure they import the ``TTToken`` and ``TTTime`` modules correctly.

.. note:: Modifying the clock tagged onto a token with a
    ``TTExecuteOnFullToken`` -enabled SQ will produce a runtime error; this is
    to avoid side effects for other SQs.



