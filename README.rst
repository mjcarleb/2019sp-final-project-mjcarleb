========
Overview
========

.. start-badges

.. list-table::
    :stub-columns: 1

    * - docs
      - |docs|
    * - tests
      - | |travis| |appveyor|
        |
    * - package
      - | |version| |wheel| |supported-versions| |supported-implementations|
        | |commits-since|
.. |docs| image:: https://readthedocs.org/projects/2019sp-airflow_project-mjcarleb/badge/?style=flat
    :target: https://readthedocs.org/projects/2019sp-airflow_project-mjcarleb
    :alt: Documentation Status

.. |travis| image:: https://travis-ci.org/csci-e-29/2019sp-airflow_project-mjcarleb.svg?branch=master
    :alt: Travis-CI Build Status
    :target: https://travis-ci.org/csci-e-29/2019sp-airflow_project-mjcarleb

.. |appveyor| image:: https://ci.appveyor.com/api/projects/status/github/csci-e-29/2019sp-airflow_project-mjcarleb?branch=master&svg=true
    :alt: AppVeyor Build Status
    :target: https://ci.appveyor.com/project/csci-e-29/2019sp-airflow_project-mjcarleb

.. |version| image:: https://img.shields.io/pypi/v/airflow-project.svg
    :alt: PyPI Package latest release
    :target: https://pypi.org/project/airflow-project

.. |commits-since| image:: https://img.shields.io/github/commits-since/csci-e-29/2019sp-airflow_project-mjcarleb/v0.0.0.svg
    :alt: Commits since latest release
    :target: https://github.com/csci-e-29/2019sp-airflow_project-mjcarleb/compare/v0.0.0...master

.. |wheel| image:: https://img.shields.io/pypi/wheel/airflow-project.svg
    :alt: PyPI Wheel
    :target: https://pypi.org/project/airflow-project

.. |supported-versions| image:: https://img.shields.io/pypi/pyversions/airflow-project.svg
    :alt: Supported versions
    :target: https://pypi.org/project/airflow-project

.. |supported-implementations| image:: https://img.shields.io/pypi/implementation/airflow-project.svg
    :alt: Supported implementations
    :target: https://pypi.org/project/airflow-project


.. end-badges

An example package. Generated with cookiecutter-pylibrary.

* Free software: BSD 2-Clause License

Installation
============

::

    pip install airflow-project

Documentation
=============


https://2019sp-airflow_project-mjcarleb.readthedocs.io/


Development
===========

To run the all tests run::

    tox

Note, to combine the coverage data from all the tox environments run:

.. list-table::
    :widths: 10 90
    :stub-columns: 1

    - - Windows
      - ::

            set PYTEST_ADDOPTS=--cov-append
            tox

    - - Other
      - ::

            PYTEST_ADDOPTS=--cov-append tox
