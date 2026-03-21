# TickTalk Python (TTPython)

MIT License

[TTPython Docs](<REDACTED>)

[Mailing List](<REDACTED>)

## Installation

TTPython runs in Python 3, and has been tested on Python >=3.6.  We recommend
using a virtual environment manager like
[Conda](https://www.anaconda.com/products/individual) or
[python virtual environments](https://docs.python.org/3/tutorial/venv.html)

Install the dependencies for TTPython

```
pip install -r requirements.txt
```

If you wish to use the graphviz package for graph visualization during
compilation (when calling compile, setting use_graphviz=True), then that package
must be installed with a few extra steps. The package "graphviz" will be
installed by pip, but a corollary (and necessary) package
[pygraphviz](https://pygraphviz.github.io/) requires a few additional steps.
On Mac, we find 'brew' to be the best avenue for the initial build process,
which requires more than just pip or conda installations.

## Basic Testing and Verification

In the top level of the repo, there is a Jupyter notebook
(```TickTalkTest.ipynb```) that will compile and simulate a simple TTPython
example to help get you started. This is helpful for verifying the
installation process worked correctly.

Open the Jupyter notebook and run the first two blocks within the notebook.

The first will run a set of basic regression tests and compile a TTPython program
in the 'examples' directory called ```streaming_merge```. This program simply
produces two streams of values representing sinusoids sampled every 500 ms, and
adds them together. It also computes a moving average on one of the sinusoid
streams.

The second block in the notebook will interpret the graph within a simulated
environment. Note that this may be quite slow within Jupyter, taking up to 40
seconds to finish (running from terminal is about 2 orders of magnitude faster).
If it finishes correctly, it will produce two output graphs depicting the added
sinusoids and the moving average.

## Tutorial
We have included a Jupyter notebook that is designated to be the tutorial for
TTPython. The environment has been configured to work in Colab, which eases
testing TTPython on a nontrivial program. The tutorial is intended to be used
along with the written tutorial on the webpage, found
[here](<REDACTED>).

## Running a TTPython Program

We have provided a set of scripts to help you compile and run your application.
You should use the script `compile.py` to compile your TTPython program to a
pickle file, which you will use it as an argument to either `runrtm.py` or
`simulate.py` depending on which type of representation of time you choose.
All scripts have the `-h` option to describe how to use the script.  Use
`simulate.py` for testing purposes on a single device, as it uses the simpy
environment to simulate timing. This will give a deterministic execution of
your application.`runrtm.py` uses the Python time library and should be used
for deployment of your application. To deploy your application, you will need
each device in your application have TTPython and the source code. First,
start the TTPython runtime on one device with `runrtm.py`. You can now have
devices subscribe to this runtime with `runens.py`. Once all the devices are
connected, you can continue exeuction with `runrtm.py`.
