from setuptools import setup, find_packages
from os.path import join, dirname

setup(
    name='ar_ex_sys_worker',
    version='0.2.82',
    packages=find_packages(),
    author='PunchyArchy',
    author_email='ksmdrmvscthny@gmail.com',
    long_description=open(join(dirname(__file__), 'README.txt')).read(),
    include_package_data=True,
    install_requires=[
        'requests',
        'wsqluse==1.85',
        'pytz',
        'tzlocal'
    ],
)
