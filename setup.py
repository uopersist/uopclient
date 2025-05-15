__author__ = 'samantha'
from setuptools import setup, find_packages

setup(name='uopclient',
      version='0.1',
      description='python client bases for UOP use',
      author='Samantha Atkins',
      author_email='samantha@conceptwareinc.com',
      license='internal',
      packages=find_packages(exclude=['tests', 'docs']),
      install_requires = ['validators', 'requests', 'uop', 'sjautils'],
      zip_safe=False)
