# Python Samples

This directory contains two bridge-specific sample sets:

- `JayDeBeApi/`: SQL-first Python samples using JayDeBeApi + JDBC.
- `JPype/`: direct Java API samples using JPype.

## JayDeBeApi

Install dependency:

pip install JayDeBeApi

Run any sample (example):

python JayDeBeApi/SyncliteDeviceApp.py

## JPype

Install dependency:

pip install jpype1

Run any sample (example):

python JPype/SyncLiteStoreAPIApp.py

JPype API-style samples include:

- SyncLiteStoreAPIApp.py
- SyncLiteStreamAPIApp.py
- SyncLiteKafkaProduceAPIApp.py
- SyncLiteJedisAPIApp.py

Notes:
- Samples default to SQLite and include inline comments for replacing SQL-device/appender device types.
- Replace synclite-logger-<version>.jar in scripts with your actual jar name/path.
- Keep synclite_logger.conf in the current working directory.
