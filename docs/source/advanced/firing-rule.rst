Dataflow Firing Rules
=============================

.. _firing-rule:

Firing rules dictate when and on what inputs an SQ will execute. Firing rules
form the synchronization barriers in a dataflow model of computation, and are
one of the main areas that TTPython and *timed dataflow* differs from prior
literature. We use firing rules for several control-plane actions that dataflow
graphs otherwise have difficulty performing, like reinvoking themselves with
some periodicity. There are several different firing rules, and we will explore
the syntax and semantics of each in this section. In time (pun not intended),
more firing rules may be added here to account for mechanisms that become
important enough to have their own firing rule.

An SQ's firing rule is checked every time a new token arrives.

Timed Firing Rule
--------------------

The ``Timed`` Firing Rule is the default firing rule, and requires no additional
specification, aside from the ``@SQify`` function decorator. This firing rule
assumes that any amount of concurrence is sufficient for finding tokens to
execute on. In TTPython, we label all tokens with time intervals (``TTTime``)
that represent a period of time that the value within the token is relevant for.
In this firing, it has no relation to real or wall-clock time; it simply
provides a *temporal context*.

Our notion of concurrency is therefore based on an overlap of this time
intervals: When the time intervals on a token have some non-zero intersection,
there is some degree of concurrency in the data. By default, any amount of
concurrency counts, although we prefer more than less: the best canditate tokens
have maximal intersection with each other.

In this way, the firing rule is satisfied when every input port can offer a
token that has an intersection with the prospective tokens from all other ports
(the search originates from the newest input token). If there are multiple
candidate sets, the returned set will be the one with the largest interval
width.

When the firing rule is passed, the tokens are copied into a
``TTExecutionContext``, which the ``TTExecuteProcess`` will accept before
running the SQ. The input ports may be designated as Sticky or non-Sticky; in
the former case the copied tokens are left in intermediate storage within the
``TTSQSync`` and in the latter case, they are removed. The Sticky vs. non-Sticky
designation is decided based on whether the upstream SQ does or does not process
on streams of input data (where is a stream is produced by an SQ with the
'TimedRetrigger` rule, which we'll see next).

TimedRetrigger Firing Rule
---------------------------

The ``TimedRetrigger`` firing rule is used to generate streams of data,
operating as a :ref:`Trigger in, N Out<sqpattern>` SQ. It is automatically set
as the firing rule for an SQ function decorated with ``@STREAMify``.

SQs with this firing rule behave by repeatedly calling the ``@STREAMify'd``
function according to some period and phase. This will produce values with
``TTTime`` tags whose interval is centered on when execution actually occurred
and is of some programmer-set width. Each of these parameters, as well as the
clock domain, is set using :ref:`TTPython meta-parameters<args-and-params>`,
which we'll describe here.

Consider the following example, in which a @STREAMify'd function is
parameterized for a period, phase, clock domain, and data validity interval:

.. code-block:: Python3

    @GRAPHify
    def generate_stream(trigger):
        with TTClock('root') as ROOT_CLOCK:
            return read_sensor(trigger, TTClock=ROOT_CLOCK, TTPeriod=1000000, TTPhase=500000, TTDataValidityInterval=100000)

In this example, the stream-generating SQ 'read_sensor' is told to use
time-values in the ROOT_CLOCK domain by providing that clock's assigned value in
the ``TTClock`` argument (yes, an overloading of the ``TTClock`` used elsewhere
in the TTPython programming framework). It will trigger itself based on the
``TTPeriod`` of 1,000,000 ticks of the provided clock (1 second for the default
root clock), specifically on the 500,000 th tick of that clock, modulo the
period. This period and phase set the time at which all ensembles with this SQ
should take a sample. The ``TTDataValidityInterval`` informs the
``TTExecuteProcess`` how to generate a new time-interval for this sample in the
stream. It approximates the sampling time by finding the midpoint of when the SQ
started and stopped computing its sampling function. The new interval is this
timestamp, plus-minus the ``TTDataValidityInterval`` divided by 2. The width of
the resulting interval is then ``TTDataValidityInterval``.

This configures the steady-state streaming behavior, but does not encode how
long this should go on. This information is obtained from the inputs. All inputs
to ``TimedRetrigger`` firing rules are understood as 'Sticky', and the
intersection between those input tokens defines the start and stop time for
stream generation. For example, suppose the 'trigger' input arrived with a
time-interval (0, 10,000,000). This SQ would produce outputs for the stream
within this period, attempting to have each output timestamp centered on
XX,500,000 ticks. Note that if we have already passed some of those desired
output times, we will not attempt to retroactively satisfy them; we only concern
ourselves with meeting future requirements for stream generation. To reiterate,
the start and stop times are real-time upper and lower bounds, and we will not
produce tokens when the provided clock reads outside of those bounds. We may
start producing values for the new stream when the input tokens meet an initial
``Timed`` firing rule check.

To do this retriggering, a feedback token is created locally in the
synchronization process (``TTInputTokenProcess``) and delayed such that it will
be released into the process's input queue at the time the next value is to be
produced.

Two things to note here.

1. This desired triggering time is best-efforts: in all likelihood, processing delays will actually produce the tokens slightly later than desired; the phase can be modified, but this is a bandaid over the real issue.
2. Long processing delays within the ``TTExecuteProcess`` may lead to backlog/queueing and infeasible input synchronization in downstream SQs; this may suggest a poorly chosen mapping, an infeasible application, or that the ``TTExecuteProcess`` should apply a form of flow control.

More information is accessible in the :ref:`corresponding
tutorial<tutorial-streamify>`.


SequentialRetrigger Firing Rule
--------------------------------

In a streaming context, running an SQ that employs static variables and stateful
behavior generally has an assumed behavior with respect to the order in which
values are processed. That is, chronologically. Imagine a digital low-pass
filter: the phase and frequency characteristics of the output signal are
contingent on the input signal being processed as a time-series signal. Running
iteration 1, then 2, then 3, then 5, then 4 will produce a different result then
1, 2, 3, 4, 5. The order of processing makes a difference when state is
involved.

When the programmer declares their SQ will be stateful (by including ``global
sq_state`` as shown in the example below), we will use this
``SequentialRetrigger`` rule.

.. code-block:: Python3

    @SQify
    def moving_average_filter(new_input):
        global sq_state # an empty dictionary when first created

        # set default values for average and counter, if not present in dictionary 'sq_state'
        if sq_state.get('average', None) is None: sq_state['average'] = 0
        if sq_state.get('count', None) is None: sq_state['count'] = 0

        sq_state['average'] = (sq_state['average'] * sq_state['count'] + new_input) / (sq_state['count'] + 1)
        sq_state['count'] += 1

        return sq_state['average']

This SQ will be analyzed by the compiler and will see ``global sq_state`` in the
function definition and that the source of ``new_input`` is a stream (of
tokens). As such, the compiler assumes this SQ should run chronologically on the
set of inputs, such that each execution of the SQ will produce a feedback
control token such that only newer tokens (i.e., those with a higher 'start
tick' in the ``TTTime`` portion of the token) can be used for subsequent
processing. This forces the SQ to only operate on chronological inputs.

However, the firing rule is not perfect in that it can skip iterations in the
stream if they arrive too far out of order (in time). A frequency-sensitive
algorithm may not handle this well, especially if it does not use the ``TTTime``
values directly (see :ref:`meta-parameters<args-and-params>` for
``TTExecuteOnFullToken``). However, the alternative requires much
parameterization from the user, including how long to wait for an input before
moving on, a default value to use, the assumed periodicity of the input stream,
etc. In time, this may be replaced by a variation of ``TTDeadline``.

.. comment: Is this solving an imaginary problem (poorly?) Deadlines on every SQ
    seems heavy-handed, but are probably more precise. The extant strategy is
    loosely goosey, but requires no input from the programmer.

More information is accessible in the :ref:'corresponding tutorial<tutorial-streamstateful'.

A Note for Future Development Efforts
---------------------------------------

Firing rules in the Dataflow Process Network literature are multifaceted and
allow several rules to be applied in conjunction (AND, OR, etc.). Some of our
rules are too complex in their use of real-time to easily permit this, but it is
worth architectural consideration as to how this could be implemented. For
instance, the ``SequentialRetrigger`` rule could be applied optionally to both
``Timed`` and ``TimedRetrigger``.

Future development effort should also be spent on accounting for what happens
when there are multiple application contexts or when a stream producer is
invoked to produce samples for more than one interval (even if those intervals
are totally distinct).

A deadline-cognizant firing rule is also a current Work in Progress, and unlike
most WIP mechanisms, is mentioned in this documentation because it is a
fundamental component of time-sensitive architectures. When that is complete,
the syntax and semantics will be described in this section and have its own
tutorial page, :ref:`Deadline Tutorial<tutorial-deadlines>`.
