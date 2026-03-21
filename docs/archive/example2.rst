Second Example
==============

The first example illustrated how to create a graph with a single ``SQ``.  Let's go a little deeper and try to create a graph with more parallelism.  

Primitive Operations as SQs
---------------------------

In order to do so, we will introduce some primitive functions for addition, subtraction and so on as simple but clear ``@SQify``-ed functions.  We define the following:

.. code-block:: Python3

    @SQify
    def ADD(a, b):
        '''Addition'''
        return a+b

    @SQify
    def SUB(a, b):
        '''Subtraction'''
        return a-b

    @SQify
    def MULT(a, b):
        '''Multiplication'''
        return a*b

    @SQify
    def DIV(a, b):
        '''Division'''
        return a/b

    @SQify
    def SQRT(a):
        '''Square root'''
        return sqrt(a)

    @SQify
    def CONST(trigger, const=None):
        '''A constant value'''
        return const

As you can see, these vanilla Python functions do the obvious thing -- but by ``@SQify``-ing them, we turn them into functions that accept and produce tokens, not just values.  There is some funny business with ``CONST``, but let's set that aside for the moment.  Now, let's compose a TTPython graph with these:

.. code-block:: Python3

    @GRAPHify
    def main(a, b, c):          
        const_4 = CONST(a, const=4)
        val_ac = MULT(a, c)
        val_4ac = MULT(const_4, val_ac)
        sqrt_term = SQRT(SUB(MULT(b, b), val_4ac))
        a_times_2 = MULT(CONST(a, const=2), a)
        neg_b = SUB(CONST(a, const=0), b)
        root_1 = DIV(ADD(neg_b, sqrt_term), a_times_2)
        root_2 = DIV(SUB(neg_b, sqrt_term), a_times_2)
        return TUPLE_2(root_1, root_2)

Compiled Result
---------------

In this program, the ONLY functions are ``@SQify``-ed as we expect.  What does it look like when compiled?

.. image:: ../_static/quadratic-graph-many-sq.jpg
  :width: 400

Infix
-----

Much more parallel!  But the program has gotten ugly with all those ``SQ`` calls.  Not to worry -- with a little bit of abstract syntax tree magic, we allow the programmer to write in a more natural style in which binary operations like add and subtract can use Python infix notation, these same infix operators can be overloaded to handle tokens as their inputs, not just scalars, and constants can be turned into constant-generating nodes that are auto-triggered.  The code then looks like this:

.. code-block:: Python3

    @GRAPHify
    def main(a, b, c):     
        sqrt_term = SQRT((b * b) - 4 * a * c)
        a_times_2 = 2 * a
        root_1 = (-b + sqrt_term) / a_times_2
        root_2 = (-b - sqrt_term) / a_times_2
        return TUPLE_2(root_1, root_2)

... almost, but not quite, entirely unlike a real programming language.  

The compiled graph is the same as above.   