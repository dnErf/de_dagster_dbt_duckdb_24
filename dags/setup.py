from setuptools import find_packages, setup

setup(
    name="src",
    packages=find_packages(exclude=["_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
