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
