from setuptools import setup, find_packages

setup(
    name='ctxdashboard',
    version='1.0',
    packages=find_packages(include=["ctxdashboard"]),
    test_suite='test_package',
    install_requires=[
        "matplotlib>=3.6.2",
        "matplotlib-inline>=0.1.6",
        "numpy>=1.23.4",
        "openpyxl>=3.0.10",
        "pandas>=1.5.2",
        "seaborn>=0.12.1",
        "flask",
        "ctxfitness",
        "pyxtension==1.13.16"
    ]
)
