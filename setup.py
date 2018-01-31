#!/usr/bin/env python

from __future__ import with_statement

from setuptools import setup

with open("README") as readme:
    documentation = readme.read()

setup(
    name="deluge-client-sync",
    version="1.0.0",
    description="A synchronous API to deluge using normal python code (no "
    "TwistedMatrix, no asyncio)",
    long_description=documentation,
    author="AllSeeingEyeTolledEweSew",
    author_email="allseeingeyetolledewesew@protonmail.com",
    license="Unlicense",
    py_modules=["deluge_client_sync"],
    url="http://github.com/allseeingeyetolledewesew/deluge_client_sync",
    use_2to3=True,
    install_requires=[
        "rencode>=1.0.0",
        "pyxdg>=0.25",
        "futures>=3.1.1;python_version<\"3.2\"",
    ],
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: Public Domain",
        "Programming Language :: Python",
        "Topic :: Communications :: File Sharing",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: System :: Networking",
        "Operating System :: OS Independent",
        "License :: Public Domain",
    ],
)
