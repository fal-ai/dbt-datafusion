#!/usr/bin/env python
from setuptools import find_namespace_packages, setup

package_name = "dbt-datafusion"
# make sure this always matches dbt/adapters/{adapter}/__version__.py
package_version = "1.2.0"
description = """The DataFusion adapter plugin for dbt"""

setup(
    name=package_name,
    version=package_version,
    description=description,
    long_description=description,
    author="Burkay Gur",
    author_email="burkay@fal.ai",
    url="https://github.com/fal-ai/dbt-datafusion",
    packages=find_namespace_packages(include=["dbt", "dbt.*"]),
    include_package_data=True,
    install_requires=[
        "dbt-core~=1.2.0",
    ],
)
