/*
 * Copyright (c) 2024 mahendra.chavan@synclite.io, all rights reserved.
 *
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied.  See the License for the specific language governing permissions and limitations
 * under the License.
 *
 */

package io.synclite.logger;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.HashMap;

class PropsLoader {

    HashMap<String, String> properties = new HashMap<String, String>();
    void loadProperties(Path propsPath) throws SQLException {
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(propsPath.toFile()));
            String line = reader.readLine();
            while (line != null) {
                line = line.trim();
                if (line.trim().isEmpty()) {
                    line = reader.readLine();
                    continue;
                }
                if (line.startsWith("#")) {
                    line = reader.readLine();
                    continue;
                }
                String[] tokens = line.split("=");
                if (tokens.length < 2) {
                    throw new SQLException("Invalid line in property file " + propsPath + " : " + line);
                }
                properties.put(tokens[0].trim().toLowerCase(), line.substring(line.indexOf("=") + 1, line.length()).trim());
                line = reader.readLine();
            }
        } catch (IOException e) {
            throw new SQLException("Failed to load property file : " + propsPath + " : ", e);
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    throw new SQLException("Failed to close property file : " + propsPath + ": " , e);
                }
            }
        }
    }

}
