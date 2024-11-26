from setuptools import setup, find_packages

setup(
    name='tvp',
    version="0.1.0",
    author="rphadtare",
    url='https://databricks.com',
    author_email='rohitphadtare39@gmail.com',
    description='tvp-wheel',
    packages=find_packages(include=['tvp']),
    install_requires=[
        'setuptools'
    ]
)

##
# To execute and create whl file use following command -
# python3 setup.py bdist_wheel
#
# If error occurs - ModuleNotFoundError: No module named 'setuptools'
# Run following command on terminal to fix setuptools - brew install python-setuptools
# #