#!/usr/bin/env python
# Since the twitter tool's requirements are tied to a specific
# version of PyYaml or simplejson, this simple script should
# be able to install
import os
import sys
# make sure we have setup tools installed
try:
    from setuptools.command import easy_install
except:
    print """
Failed to import setuptools
 Could not find setuptools, please make sure
 that setuptools is installed on your machine
    """
    exit(1)

# required packages
packages = [
    'simplejson',
    'PyYaml',
    ]

# on non windows systems make sure we are running as root
if not sys.platform in ['win32', 'win64']:
    uid = os.geteuid()
    if not uid == 0:
        print """
Failed to run as Root.
 On  non-Windows systems, you must run this script with
 sudo privileges like;
  >  sudo %s
        """ % os.path.basename(__file__) 
        exit(1)

# install all of the modules with setuptools
for p in packages:
    easy_install.main(['-U', p])
