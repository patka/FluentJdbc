package com.github.fluentjdbc;

import java.sql.SQLException;

/**
 * This is just a wrapper class for the checked
 * <code>SQLException</code>. Since this is a runtime exception
 * there is no need for an explicit try-catch block.
 *
 * @author Patrick Kranz
 */
public class JdbcException extends RuntimeException {
    public JdbcException(String message, SQLException innerException) {
        super(message, innerException);
    }
}
