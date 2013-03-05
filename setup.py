# -*- Mode: Python -*-

from setuptools import setup, find_packages
setup (
    name              = 'amqp_shrapnel',
    version           = '0.5.1',
    packages          = find_packages(),
    author            = 'Sam Rushing',
    description       = "AMQP for Shrapnel",
    license           = "Simplified BSD",
    keywords          = "amqp shrapnel",
    url               = 'http://github.com/mkushnir/amqp-shrapnel/',
    download_url      = "http://github.com/mkushnir/amqp-shrapnel/tarball/v0.5#egg=amqp-shrapnel",
    install_requires  = ['coro'],
    dependency_links  = ['http://github.com/ironport/shrapnel/tarball/master#egg=coro-0.1'],
    zip_safe          = False
    )
