import os
from subprocess import Popen, PIPE

from setuptools import setup, find_packages, Extension


def pkgconfig(*packages, **kw):
    flag_map = {'-I': 'include_dirs', '-L': 'library_dirs', '-l': 'libraries'}
    cmd = "pkg-config --libs --cflags %s" % ' '.join(packages)
    p = Popen(cmd, shell=True, stdin=PIPE, stdout=PIPE)
    res = p.stdout.read().decode('utf-8')
    p.wait()
    for token in res.split():
        kw.setdefault(flag_map.get(token[:2]), []).append(token[2:])
    return kw

version = '0.4'

here = os.path.abspath(os.path.dirname(__file__))

README = open(os.path.join(here, 'README.rst')).read()
CHANGES = open(os.path.join(here, 'CHANGES.rst')).read()

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
      ext_modules=[Extension(
          '_groonga',
          sources=['_groonga.c'],
          define_macros=[],
          **pkgconfig('groonga')
          )],
      )
