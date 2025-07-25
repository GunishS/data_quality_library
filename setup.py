from setuptools import setup, find_packages

setup(
    name="dq_library",
    version="0.1.0",
    description="A Python library for data quality checks for different layers that can be intregated and used with multiple artifacts like data pipeline, notebook etc.",
    author="Gunish Swarnkar",
    packages=find_packages(),
    install_requires=[
        "pyspark"
    ],
    keywords=['data quaity','data testing','quality of data']
    python_requires=">=3.7",
    classifiers=[
        "Development Status :: Testing",
        "Intented Audience :: Developers"
    ]
)
