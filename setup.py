#!/usr/bin/env python

from distutils.core import setup

setup (
	name='icecat-proxy',
	version='1.0',
	description=open('README.md', 'r').read(),
	author='alex bodnaru',
	author_email='alexbodn@gmail.com',
	url='http://www.resheteva.org',
	packages=['icecat_proxy'],
	requires: [
		'requests-cache', 
	]
)

