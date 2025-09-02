'''
The setup.py file is an essential part of any Python project. 
It defines the package metadata and dependencies, 
making it easier to distribute and install the package. 
In this example, we will create a basic setup.py file 
for our network security project.
'''
from setuptools import setup, find_packages

# find_packages will automatically discover all packages and subpackages
# in your project directory, so you don't have to manually specify them.
# Wherever you have an __init__.py file, that directory will be considered a package.

def get_requirements(filename):
    '''Reads a requirements file and returns a list of packages.'''
    requirement_lst=[]
    try:
        with open(filename, "r") as file:
            requirements = file.readlines()
            for line in requirements:
                requirement= line.strip()
                if requirement and requirement != "-e .":
                    requirement_lst.append(requirement)
    except FileNotFoundError:
        print(f"Warning: {filename} not found. No dependencies will be installed.")
        return []
    return requirement_lst

setup(
    name="NetworkSecurity",
    version="0.1",
    author="Gundeti Siddarth",
    author_email="gundetisiddarth@gmail.com",
    packages=find_packages(),
    install_requires=get_requirements("requirements.txt")
    ,
)