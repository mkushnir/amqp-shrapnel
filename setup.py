# -*- Mode: Python -*-

from distutils.core import setup
import amqp_shrapnel

setup (
    name              = 'amqp_shrapnel',
    version           = amqp_shrapnel.__version__,
    packages          = ['amqp_shrapnel'],
    author            = 'Sam Rushing',
    description       = "AMQP for Shrapnel",
    license           = "Simplified BSD",
    keywords          = "amqp shrapnel",
    url               = 'http://github.com/mkushnir/amqp-shrapnel/',
    )
