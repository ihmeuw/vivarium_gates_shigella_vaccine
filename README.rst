===============================
vivarium_gates_shigella_vaccine
===============================

Research repository for the vivarium_gates_shigella_vaccine project.

.. contents::
   :depth: 1

Model Documentation Resources
-----------------------------

**You should put links to the concept model documentation and any other**
**relevant documentation here.**

Installation
------------

These models require data from GBD databases. You'll need several internal
IHME packages and access to the IHME cluster.

To install the extra dependencies create a file called ~/.pip/pip.conf which
looks like this::

    [global]
    extra-index-url = http://pypi.services.ihme.washington.edu/simple
    trusted-host = pypi.services.ihme.washington.edu


To set up a new research environment, open up a terminal on the cluster and
run::

    $> conda create --name=vivarium_gates_shigella_vaccine python=3.6
    ...standard conda install stuff...
    $> conda activate vivarium_gates_shigella_vaccine
    (vivarium_gates_shigella_vaccine) $> conda install redis
    (vivarium_gates_shigella_vaccine) $> git clone git@github.com:ihmeuw/vivarium_gates_shigella_vaccine.git
    ...you may need to do username/password stuff here...
    (vivarium_gates_shigella_vaccine) $> cd vivarium_gates_shigella_vaccine
    (vivarium_gates_shigella_vaccine) $> pip install -e .


Usage
-----

You'll find four directories inside the main
``src/vivarium_gates_shigella_vaccine`` package directory:

- ``components``

  This directory is for Python modules containing custom components for
  the vivarium_gates_shigella_vaccine project. You should work with the
  engineering staff to help scope out what you need and get them built.

- ``data``

  If you have **small scale** external data for use in your sim or in your
  results processing, it can live here. This is almost certainly not the right
  place for data, so make sure there's not a better place to put it first.
  Otherwise, this is the place to put data processing tools and scripts.

- ``model_specifications``

  This directory should hold all model specifications and branch files
  associated with the project.

- ``verification_and_validation``

  Any post-processing and analysis code or notebooks you write should be
  stored in this directory.

