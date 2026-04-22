# Java Samples

Compile samples against the built logger jar:

javac -cp ..\\..\\target\\synclite-logger-oss.jar *.java

Run any sample (example):

java -cp ..\\..\\target\\synclite-logger-oss.jar;. SyncliteDeviceApp

Notes:
- Samples default to SQLite and include inline comments for replacing SQL-device/appender device types.
- Keep synclite_logger.conf in the current working directory.
