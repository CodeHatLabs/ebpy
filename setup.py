import os
from setuptools import setup, find_packages


setup(
    name='ebpy',
    version=0.0,
    author='Code Hat Labs, LLC',
    author_email='dev@codehatlabs.com',
    url='https://github.com/CodeHatLabs/ebpy',
    description='Python tools for Elastic Beanstalk Web/Worker Deployments',
    packages=find_packages(),
    long_description="",
    keywords='python',
    zip_safe=False,
    install_requires=[
        'boto3',
    ],
    test_suite='',
    include_package_data=True,
    classifiers=[
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Topic :: Software Development'
    ],
)
