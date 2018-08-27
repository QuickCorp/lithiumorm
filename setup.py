#!/usr/bin/python
# -*- coding: utf-8 -*-
from distutils.core import setup
setup(
  name = 'Lithium-ORM',
  packages = ['lithium'], # this must be the same as the name above
  version = '0.0.1',
  description = 'QuickCorp Lithium ORM',
  author = 'Jean Machuca',
  author_email = 'correojean@gmail.com',
  url = 'https://github.com/QuickCorp/lithiumorm.git', # use the URL to the github repo
  download_url = 'https://github.com/QuickCorp/lithiumorm/archive/master.zip', # I'll explain this in a second
  keywords = ['ndb', 'gae', 'sqlite','MySQLdb','ORM','easy','google','app','engine','sqlite3'], # arbitrary keywords
  classifiers=[
      'Development Status :: 4 - Beta',
      'Environment :: Console',
      'Environment :: Web Environment',
      'Intended Audience :: End Users/Desktop',
      'Intended Audience :: Developers',
      'Intended Audience :: System Administrators',
      'License :: OSI Approved :: Python Software Foundation License',
      'Operating System :: MacOS :: MacOS X',
      'Operating System :: Microsoft :: Windows',
      'Operating System :: POSIX',
      'Programming Language :: Python',
      'Topic :: Communications :: Email',
      'Topic :: Office/Business',
      'Topic :: Software Development :: Bug Tracking',
      ],
  install_requires=['mysqlclient','MySQL-python','pymysql']
)
# patch distutils if it can't cope with the "classifiers" or
# "download_url" keywords
from sys import version
if version < '2.2.3':
    from distutils.dist import DistributionMetadata
    DistributionMetadata.classifiers = None
    DistributionMetadata.download_url = None
