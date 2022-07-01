import os
from setuptools import setup


setup(
    name='candymill',
    version='0.0.1',
    author='',
    scripts=['bin/candymill-hashfilter'],
    packages=['candymill', ],
    install_requires=[r.strip() for r in open("requirements.txt").readlines()],
)
