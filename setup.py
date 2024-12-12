from setuptools import setup, find_packages

setup(
    name="Glaciation_time_estimator",
    version="0.1.0",
    description="A Python library for estimating cloud glaciation times.",
    author="Dragomir Nikolov",
    author_email="dnikolo@student.ethz.ch",
    url="https://github.com/yourusername/Glaciation_time_estimator",  # Update with your repository URL
    packages=find_packages(
        include=["Auxiliary_func", "Data_preprocessing", "Data_postprocessing"]
    ),  # Only include the relevant packages
    include_package_data=True,
    install_requires=[
        "numpy",  # List your dependencies here
    ],
    python_requires=">=3.11",
)