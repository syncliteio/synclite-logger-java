# Copyright (c) 2024 mahendra.chavan@synclite.io, all rights reserved.
#
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
# in compliance with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License
# is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
# or implied.  See the License for the specific language governing permissions and limitations
# under the License.
#
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
