from setuptools import setup, find_packages

setup(
    name='asb-tour',
    version='0.1.0',
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'click',
        'azure-servicebus',
    ],
    entry_points={
        'console_scripts': [
            'asb-tour = asb_tour.scripts.cli_script:cli',
        ],
    },
)
