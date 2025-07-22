from setuptools import setup, find_packages

setup(
    name="dq_library",
    version="0.1.0",
    description="A Python library for data quality checks in different layers can be intregated and used with multiple artiffacts like data pipeline, notebook etc.",
    author="Gunish Swarnkar",
    packages=find_packages(),
    install_requires=[
        "pyspark"
    ],
    python_requires=">=3.7"
)
