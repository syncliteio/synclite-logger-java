package io.synclite.logger;

import java.sql.SQLException;

public class RemoteShippingException extends SQLException {

    public RemoteShippingException(String message) {
        super(message);
    }

    public RemoteShippingException(Exception e) {
        super(e);
    }

    public RemoteShippingException(String message, Exception e) {
        super(message, e);
    }

}
