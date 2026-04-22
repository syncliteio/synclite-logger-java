"""
Shared JPype bootstrap helper.

This helper starts the JVM once and makes the SyncLite Java classes available
directly from Python. All JPype samples build on that bridge before they open
devices or invoke SyncLite APIs.

The important distinction from JayDeBeApi is that JPype exposes the actual Java
API objects, while JayDeBeApi stays at the JDBC and SQL layer.
"""

import jpype
import jpype.imports


def start_jvm(jar_path="synclite-logger-<version>.jar"):
    if not jpype.isJVMStarted():
        jpype.startJVM(classpath=[jar_path])
