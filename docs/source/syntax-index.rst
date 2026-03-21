******
Syntax
******

.. toctree::
   :maxdepth: 2
   :hidden:

   syntax/syntax-definition
   syntax/syntax-with
   syntax/syntax-sqify
   classes/instructions

A TTPython program is a textual representation of a set of ``SQ`` templates
(think of this as a kind of library) and a textual representation of the graph
that stitches together a bunch of ``SQ`` instances in a structured way, possibly
with some annotations related to time and other meta-data.  At the most basic
level, TTPython *looks* like Python but with a couple of important differences.
While TTPython takes advantage in many ways of Python syntax, some parts of a
TTPython program can be honest-to-goodness Python while others are subject to
grammatical rules specific to TTPython as its own language.