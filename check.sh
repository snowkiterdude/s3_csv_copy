#!/bin/bash
echo
echo "s3_csv_copy.py"
black s3_csv_copy.py
pylint s3_csv_copy.py
echo
echo "lib/__init__.py"
black lib/__init__.py
pylint lib/__init__.py
echo
echo "lib/lists.py"
black lib/lists.py
pylint lib/lists.py
