from setuptools import setup

setup(
    name='earthquake-ingestion',
    version='1.0',
    install_requires=['apache-beam[gcp]', 'requests==2.28.1'],
    packages=['.'],
    zip_safe=False
)