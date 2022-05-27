import platform
from distutils.core import setup, Extension

_ns_help = Extension('_ns_help', sources=['ns_help/_ns_help.c'], 
                      libraries=[] if platform.system() in ['Windows','Darwin'] else ['rt',])

setup(
    name='ns_help',
    version='0.1',
    author="Chris Lane",
    author_email="chris@disputingtaste.com",
    description='netstring parsing',
    license="MIT",
#    url="http://github.com/doublereedkurt/faststat",
    long_description='...',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: MIT License',
    ],
    packages=['ns_help'],
    ext_modules=[_ns_help], extra_compile_args=['-g','-ggdb'])
