from setuptools import setup, find_packages

print(find_packages())

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name='asb-cli-explorer',
    description="Small cli tool send and peek messages from Azure service bus",
    long_description=long_description,
    long_description_content_type="text/markdown",
    version='0.1.3',
    packages=find_packages(),
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
