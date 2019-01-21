from setuptools import setup, find_packages


setup(
    version="0.0.11",
    name="aiodynamo",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    install_requires=[
        "attrs>=17.4.0",
        "aiobotocore==0.6.0",  # Temporary, because of old aiohttp
        "boto3<=1.5",
        "aiohttp>3,<=3.0.9",
    ],
)
