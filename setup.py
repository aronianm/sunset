from setuptools import setup, find_packages

setup(
    name='sunset',
    version='0.1',
    description='''
                A simple importer for MySQL Database
                ''',
    author_email='aronian.m@gmail.com',
    author='Michael Aronian',
    packages=find_packages(),
    install_requires=[
        'pandas==1.5.2',
        'SQLAlchemy==1.4.45',
        'mysqlclient==2.1.1'
    ]
)