from setuptools import setup, find_packages

print(find_packages())

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name='asb-cli-explorer',
    description="Explore Azure service bus from command line",
    long_description=long_description,
    long_description_content_type="text/markdown",
    version='0.1.7',
    packages=find_packages(exclude=['tests']),
    url='https://github.com/cackharot/asb-cli-explorer',
    test_suite='tests',
    license='MIT',
    classifiers=[
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
    ],
    keywords='service bus, servicebus, azure, azure service bus, messaging, amqp',
    platforms='Python3',
    include_package_data=True,
    python_requires=">=3.7.4",
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
