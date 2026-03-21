Creating SQs from Vanilla Python Functions
==========================================

To create an SQ in TTPython, we "SQify" a function by putting @SQify in the line
above the function definition. When the SQ has the tokens it needs to run, it
will provide the values in those tokens as arguments to the supplied function.
There are a few restrictions we impose on the functions that are SQified.

However, the first argument will always be reference to the SQ object that
encapsulates all the elements of the ``TTSQ``. This allows for two essential
mechanisms: state and output.

The ``TTSQ`` object passed in will have an instance variable called 'state' that
can be used in any way the user likes to let information persist across
invocations of the SQ. They may use this to check if state variables have been
initialized, if they have been invoked on out-of-order tokens, or otherwise. The
state is entirely managed by the programmer.

Rather than using 'return', sending outputs from the SQ is explicit and
flexible. The SQ object provided to the function will have a callable
"send_token" function that will take a port number and a value to tokenize
(i.e., return) as inputs, at which point the graph interpreter will handle all
the rest of tokenizing and sending to recipient SQs. This way, the programmer
can send tokens whenever they want (and potentially multiple) without waiting
for everything to complete before otherwise returning a handful of values.

The last (known) restriction is that any function to be SQified that requires
the use of an imported library must import that library within the function
itself, not at the top of the python file.

.. currentmodule:: ticktalkpython.SQ
.. autodecorator:: SQify(function)
    :noindex:
.. autodecorator:: STREAMify(function)
    :noindex:
