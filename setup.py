'''
Created on Mar 12, 2015

@author: pete.zybrick
'''
import os
from distutils.core import setup

try:
    os.remove('MANIFEST')
except StandardError:
    pass

setup(name='awsspotbatch',
      version='1.1',
      description='IPC AWS Spot Batch Scripts',
      author='Pete Zybrick',
      author_email='pete.zybrick@ipc-global.com',
      url='https:// TODO: SVN link to branch',
      packages=['awsspotbatch', 'awsspotbatch.client', 'awsspotbatch.common', 'awsspotbatch.microsvc', 'awsspotbatch.microsvc.master', 'awsspotbatch.microsvc.request', 'awsspotbatch.run', 'awsspotbatch.setup', 'awsspotbatch.status'],
      package_dir = {'': 'src'},
     )