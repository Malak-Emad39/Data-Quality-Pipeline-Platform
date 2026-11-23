"""
Pytest configuration and global environment fixtures for Data Quality tests.
"""

import os
import sys

# Configure environment variables for PySpark and Hadoop on Windows
hadoop_dir = r"C:\Users\hp\hadoop"
if os.path.exists(hadoop_dir):
    os.environ.setdefault("HADOOP_HOME", hadoop_dir)
    hadoop_bin = os.path.join(hadoop_dir, "bin")
    if hadoop_bin not in os.environ.get("PATH", ""):
        os.environ["PATH"] = hadoop_bin + os.pathsep + os.environ.get("PATH", "")

venv_python = os.path.abspath(sys.executable)
os.environ.setdefault("PYSPARK_PYTHON", venv_python)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", venv_python)
