# AsyncIO DynamoDB

[![CircleCI](https://circleci.com/gh/HENNGE/aiodynamo.svg?style=svg)](https://circleci.com/gh/HENNGE/aiodynamo) [![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)


Asynchronous, **fast** and pythonic DynamoDB client. See [the docs](https://aiodynamo.readthedocs.io/) for details.


## Why aiodynamo

* boto3 and botocore are synchronous. aiodynamo is built for **asynchronous** apps.
* aiodynamo is **fast**. Two times faster than aiobotocore, botocore or boto3 for operations such as query or scan.
* aiobotocore is very low level. aiodynamo provides a **pythonic API**, using modern Python features. For example, paginated APIs are automatically depaginated using asynchronous iterators.
* **Legible source code**. botocore and derived libraries generate their interface at runtime, so it cannot be inspected and isn't typed. aiodynamo is hand written code you can read, inspect and understand.
* **Pluggable HTTP client**. If you're already using an asynchronous HTTP client in your project, you can use it with aiodynamo and don't need to add extra dependencies or run into dependency resolution issues.
