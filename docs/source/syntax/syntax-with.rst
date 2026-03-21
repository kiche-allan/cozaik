With Statement
==============

.. _syntax-with:

Programmers familiar with Python will recognize the ``with...`` statement as a kind of wrapper for a code block in a manner similar to the way a function decorator wraps a function.  The ``with...`` statement can be used in Python to establish a kind of execution context, for example, one in which a file is opened for reading and writing.  Before entering the block of code that makes up the body of the ``with...``, a *prolog* function runs to open the file and to bind the file handle to an identifier.  Then, when the body exits, an *epilog* block runs to close the file.  This makes for clean, well-structured, easy-to-read code with respect to handling the messiness of opening and closing the file (and dealing with errors that may come up in the process).

TTPython borrows the concept and syntax of the Python ``with...`` but gives it a different interpretation.  TTPython ``with...`` comes in three flavors:

:``with TTClock() as <symbol>``: creates a clock and potentially binds it to a symbol to be used within a block of code

:``with TTPlanB()``: establishes an error handler to be used for a block of code

:``with TTDeadline()``: establishes a deadline interval.  Unlike the other two ``with...`` constructs, ``with TTDeadline()``'s body can only be a direct invocation of a single ``SQ``.

Here is an example of our quadratic solver, showing how the body is wrapped with three *with* statements.  Note that the ``TTPlanB`` handler encompasses the entire computation but that the ``TTDeadline`` only applies to the square root computation in this example.

.. code-block:: Python3

    @GRAPHify
    def main(a, b, c):
        with TTClock.root() as CLOCK:
            with TTPlanB(planB_handler):   
                e = (b * b) - 4 * a * c         
                with TTDeadline(CLOCK, 50):      
                    sqrt_term = SQRT(e)
                a_times_2 = 2 * a
                root_1 = (-b + sqrt_term) / a_times_2
                root_2 = (-b - sqrt_term) / a_times_2
                return TUPLE_2(root_1, root_2)
