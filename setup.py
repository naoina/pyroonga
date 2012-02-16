import sys

from setuptools import setup, find_packages, Extension
from subprocess import Popen, PIPE


def pkgconfig(*packages, **kw):
    flag_map = {'-I': 'include_dirs', '-L': 'library_dirs', '-l': 'libraries'}
    cmd = "pkg-config --libs --cflags %s" % ' '.join(packages)
    p = Popen(cmd, shell=True, stdin=PIPE, stdout=PIPE)
    res = p.stdout.read().decode('utf-8')
    p.wait()
    for token in res.split():
        kw.setdefault(flag_map.get(token[:2]), []).append(token[2:])
    return kw

version = '0.2'

setup_requires = [
    'nose',
    ]
if sys.version_info[:2] < (2, 7):
    setup_requires.append('unittest2')

setup(name='pyroonga',
      version=version,
      description="Python interface for groonga",
      long_description=open('README.rst').read(),
      classifiers=[
          'Development Status :: 3 - Alpha',
          'Intended Audience :: Developers',
          'License :: OSI Approved :: BSD License',
          'Programming Language :: Python',
          'Topic :: Internet :: WWW/HTTP :: Indexing/Search',
          'Topic :: Software Development :: Libraries'
          ],
      keywords='groonga fulltext search engine',
      author='Naoya INADA',
      author_email='naoina@kuune.org',
      url='https://github.com/naoina/pyroonga',
      license='BSD',
      packages=find_packages(),
      include_package_data=True,
      zip_safe=False,
      install_requires=[
          # -*- Extra requirements: -*-
      ],
      setup_requires=setup_requires,
      test_suite='nose.collector',
      ext_modules=[Extension(
          '_groonga',
          sources=['_groonga.c'],
          define_macros=[],
          **pkgconfig('groonga')
          )],
      )
