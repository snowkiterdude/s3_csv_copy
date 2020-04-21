#!/bin/bash
black s3_csv_copy.py
pylint s3_csv_copy.py
black lib/__init__.py
pylint lib/__init__.py
