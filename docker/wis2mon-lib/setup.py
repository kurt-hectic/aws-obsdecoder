from setuptools import setup, find_namespace_packages
setup(
    name='wis2mon_lib',
    version='0.0.1',
    description='helper functions for WIS2 monitoring',
    license="Apache-2.0",
    author='Timo Pröscholdt',
    author_email='tproescholdt@wmo.int',
    install_requires=[
        'urllib3', 'jsonschema', 'requests',
        'importlib-metadata; python_version == "3.8"',
    ],
    packages=find_namespace_packages(where="src"),
    package_dir={"": "src"},
    package_data={
        "wis2mon_lib": ["*.txt", "resources/*.json", "resources/*.py"] 
    }
)
