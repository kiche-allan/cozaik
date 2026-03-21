First Example
=============

Let's take the case of generating a graph for a simple mathematical expression such as solving for the roots of a quadratic (we take the simple case and assume the solution will be two real roots).  We all recall from high school algebra that the solution to 

.. image:: ../_static/quadraticEqn.jpg
  :width: 150

is

.. image:: ../_static/quadraticSoln.jpg
  :width: 150

Writing this in vanilla Python is pretty easy: 

.. code-block:: Python3

  def quadratic_roots(a, b, c):
    sqrt_term = sqrt(b**2 - 4 * a * c)
    a_times_2 = 2 * a
    return ((-b + sqrt_term) / a_times_2, (-b - sqrt_term) / a_times_2)

@SQify-cation
-------------

The code computes and returns a tuple consisting of the two roots.  We can't use this directly to form an ``SQ`` for TTPython because the inputs and outputs are strictly values -- they are not tagged tokens.  We could force the programmer to explicitly take in and produce tokens, extracting their values prior to processing.  But that's repetitive, painful and ugly.  Saddling the programmer with the task of doing the time-matching would further complicate matters.  Python offers a very nice feature called *function decorators* that serves this purpose well.  We define a decorator called ``@SQify`` that takes a vanilla Python function as input and *wraps* it with the necessary logic for handling token manipulation, including time, allowing the programmer to focus on functionality.  TTPython does this for us, and the only change a programmer needs to make to turn vanilla Python into a legitimate ``SQ`` template is to add the @SQify decorator:

.. code-block:: Python3

  @SQify
  def quadratic_roots(a, b, c):
    sqrt_term = sqrt(b**2 - 4 * a * c)
    a_times_2 = 2 * a
    return ((-b + sqrt_term) / a_times_2, (-b - sqrt_term) / a_times_2)

That's painless enough, isn't it?  Well, almost.  We impose additional constraints on ``SQ`` *bodies* to make them behave properly in TTPython programs.  With some exceptions, we require that ``@SQify``-ed code be purely functional -- leaving behind no side effects and making no reference to *global variables* or other information not passed in explicitly as an argument.  The former condition of being side-effect-free may be a bit challenging to Python programmers who are used to passing around object instances and then mutating their internal state.  This is strictly forbidden in TTPython because it leads easily to non-deterministic results, logically speaking.  And practically speaking, if we were to permit this, it would imply the need to engage in a kind of city-scale cache coherence -- cleary something we'd rather avoid.  On a related note, symbols represent values that don't change.  This means that trying to assign a value to a symbol more than once is forbidden.  And tricks like ``a += 1`` are also illegal in TTPython.

Repeat to yourself: **data should not change, data should not change, data should not change...**

As we will see, we break with the *pure function* dictum when we come to sampling and actuating in the physical world.  

@GRAPHify-cation
----------------

But an ``SQ`` template doth not a graph make.  We need to instantiate the template within a graph.  In fact, a graph in TTPython is expressed as Python-like source in which the **ONLY** function calls are to ``@SQify``-ed functions and in which the assignment to and use of program symbols serve to establish producer-consumer relationships between ``SQ`` instances.  For this purpose, we create a second function decorator called ``@GRAPHify`` and use it like this:

.. code-block:: Python3

  @GRAPHify
  def main(a, b, c):
    return quadratic_roots(a, b, c)

Uhhh... that sure *looks* like Python.  Is it?  Actually it is not.  This bit of code is an actual TTPython graph "program" that invokes a single ``SQ``.  The difference is that within ``@SQify``-ed Python, the values are actual Python values.  In ``@GRAPHify``-ed TTPython code, **ALL** of the values are time-tagged tokens.  Since the purpose of ``@GRAPHify`` is to define the topology of a time-tagged-token graph, this should not surprise you.

Dusty Decks
-----------

This all seems like much ado about nothing.  ``@GRAPHify`` goes to lengths to make the values be tokens while the ``@SQify`` wrapper exists to unwrap these tokens.  What's the point?  Simple -- the ability to support "`dusty-deck <https://www.definitions.net/definition/dusty+deck>`_" Python.

Our goal is the creation of time-tagged dataflow graphs, and Python does not do that directly.  But at the same time, wouldn't it be nice to be able to take an old Python program and evolve it incrementally to be a full-fledged TTPython program with all the time- and parallelism-benefits that come with that?  To achieve this, we've drawn a hard line.  Existing Python code can be wrapped as a single ``SQ`` template and then instantiated.  We don't ever look inside an ``SQ``'s body at the code -- we treat it as a black box.  Like Las Vegas, what goes on inside stays inside.  This is important because it allows us to completely encapsulate code that was not designed according to TTPython rules and to use that code as a block within a TTPython program.  With time, programmers can then take that single, monolithic dusty deck code and break it up into smaller chunks that can communicate via tagged tokens.  As this is done, more of the TTPython benefits accrue.

.. warning:: Remember that ``@SQify``-ed functions need to behave like pure functions, else the non-determinism police will show up in their black helicopters.


Making Our First Graph
----------------------

Let's run the above single-``@SQify``-ed definition and ``@GRAPHify``-ed graph through the TTPython compiler and see what we get:

.. image:: ../_static/quadratic-graph-1-sq.jpg
  :width: 400

Here we have a single ``SQ`` instance (shown in green).  The inputs to the graph and the outputs from the graph are shown in red.  Not terribly interesting, but at least well-defined.  The body of that ``SQ`` is the ``@SQify``-ed function.  Passing in the values 1, 0, and -4 for *a, b,* and *c* respectively will yield the result tuple *(2, -2)* as we would expect. 