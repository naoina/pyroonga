import os
import sys
from subprocess import Popen, PIPE

from setuptools import setup, find_packages, Extension
from setuptools.command.test import test as TestCommand


def pkgconfig(*packages, **kw):
    flag_map = {'-I': 'include_dirs', '-L': 'library_dirs', '-l': 'libraries'}
    cmd = "pkg-config --libs --cflags %s" % ' '.join(packages)
    p = Popen(cmd, shell=True, stdin=PIPE, stdout=PIPE)
    res = p.stdout.read().decode('utf-8')
    p.wait()
    for token in res.split():
        kw.setdefault(flag_map.get(token[:2]), []).append(token[2:])
    return kw


def gen_pytest_class(args):
    class PyTest(TestCommand):
        def initialize_options(self):
            TestCommand.initialize_options(self)

        def finalize_options(self):
            TestCommand.finalize_options(self)
            self.test_suite = True
            self.test_args = args

        def run_tests(self):
            #import here, cause outside the eggs aren't loaded
            import pytest
            errno = pytest.main(self.test_args)
            sys.exit(errno)
    return PyTest

version = '0.5.2'

here = os.path.abspath(os.path.dirname(__file__))

README = open(os.path.join(here, 'README.rst')).read()
CHANGES = open(os.path.join(here, 'CHANGES.rst')).read()

tests_require = [
    'pytest',
]

if sys.version_info < (2, 7):
    tests_require.append('unittest2')

if sys.version_info < (3, 3):
    tests_require.append('mock')

setup(name='pyroonga',
      version=version,
      description="Python interface for groonga",
      long_description=README + '\n\n' + CHANGES,
      classifiers=[
          'Development Status :: 3 - Alpha',
          'Intended Audience :: Developers',
          'License :: OSI Approved :: MIT License',
          'Programming Language :: Python',
          'Topic :: Internet :: WWW/HTTP :: Indexing/Search',
          'Topic :: Software Development :: Libraries'
          ],
      keywords='groonga fulltext search engine',
      author='Naoya Inada',
      author_email='naoina@kuune.org',
      url='https://github.com/naoina/pyroonga',
      license='MIT',
      packages=find_packages(),
      include_package_data=True,
      zip_safe=False,
      install_requires=[
          # -*- Extra requirements: -*-
      ],
      tests_require=tests_require,
      ext_modules=[Extension(
          '_groonga',
          sources=['_groonga.c'],
          define_macros=[],
          **pkgconfig('groonga')
          )],
      cmdclass={
          'test': gen_pytest_class(['pyroonga/tests/unit']),
          'testfunctional': gen_pytest_class(['pyroonga/tests/functional']),
          'testall': gen_pytest_class(['pyroonga/tests']),
      },
      )
