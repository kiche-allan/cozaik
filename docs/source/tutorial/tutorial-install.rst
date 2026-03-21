Tutorial - Installation
=======================

.. _tutorial-install:


**Installation Using Google Colab**


Open the Jupyter Notebook file found here within Google Colab:
https://bitbucket.org/ccsg-res/ticktalkpython/src/master/tutorial.ipynb

First, run "Step 1" that will setup TTPython dependencies and external
dependencies used in the tutorial.

**Installation**


TTPython runs and has been tested on Python >=3.8.  We recommend
using a virtual environment manager like
`Conda <https://www.anaconda.com/products/individual>`_
or
`python virtual environments <https://docs.python.org/3/tutorial/venv.html>`_

Clone the TTPython repository to your local directory

.. code-block:: python

	git clone https://bitbucket.org/ccsg-res/ticktalkpython.git

Navigate to the tutorial branch and follow the steps:

1. Install the dependencies for TTPython

.. code-block:: python

	pip install -r requirements.txt


If you wish to use the graphviz package for graph visualization during
compilation (when calling compile, setting use_graphviz=True), then graphviz
must be installed with a few extra steps. The package "graphviz" will be
installed by pip, but a corollary (and necessary) package
`pygraphviz <https://pygraphviz.github.io/>`_
requires a few additional steps.
On Mac, we find 'brew' to be the best avenue for the initial build process,
which requires more than just pip or conda installations.

2. Basic Testing and Verification

In the top level of the repo, there is a jupyter notebook that will compile
and simulate a simple TTPython example to help get you started. This is helpful
for verifying the installation process worked correctly.

.. code-block:: python

	jupyter notebook TickTalkTest.ipynb



3. Open the Jupyter notebook and run the first two blocks within the notebook

The first will run a set of basic regression tests and compile a TTPython program
in the 'examples' directory called ``streaming_merge``. This program simply
produces two streams of values representing sinusoids sampled every 500 ms, and
adds them together. It also computes a moving average on one of the sinusoid
streams.

The second block in the notebook will interpret the graph within a simulated
environment. Note that this may be quite slow within jupyter, taking up to 40
seconds to finish (running from terminal is about 2 orders of magnitude faster).
If it finishes correctly, it will produce two output graphs depicting the added
sinusoids and the moving average.

**FAQ:**

**Can I run TTPython in Google Colab environment?**

Yes, Colab uses python version 3.7 as default. We have tested TTPython with
versions 3.7 to 3.9.


**Why am I getting a 'No module named' error while running the TickTalkTest.ipynb notebook?**

Make sure that all the requirements in the requirements.txt file are installed.
If you are using a conda environment, you may want to recheck the python base version.

Also note that while using a conda environment, the ast_scope package requires pip
since conda package is not available.

.. code-block:: python

	pip install ast_scope


Additionally, graphviz requires two additional 'python-graphviz' and 'pydot' packages
if you are using conda.

.. code-block:: python

	conda install python-graphviz
	conda install pydot
