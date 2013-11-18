package com.github.fluentjdbc;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * This class builds sql queries and handles the connection
 * and statement creation. It makes interaction with the
 * database easier and should reduce code duplication.
 *
 * Parameters need to be added in the order they appear in the
 * sql statement.
 *
 * If you intend to use the builder for multiple statements use
 * the continueWith() method. It will reset the parameters so
 * you can start again.
 *
 * At the end call the <code>close()</code> method to release
 * the SqlConnection. After <code>close()</code> is called
 * any further method call will result in an exception.
 *
 * @author Patrick Kranz
 */
public class SqlBuilder {
    private final Connection connection;
    private PreparedStatement preparedStatement;
    private int parameterIndex;
    private boolean isClosed;

    public SqlBuilder(DataSource dataSource) throws SQLException {
        this.connection = dataSource.getConnection();
        this.parameterIndex = 1;
        this.isClosed = false;
    }

    public SqlBuilder prepareStatement(String sqlQuery) throws SQLException {
        notNull(sqlQuery, "sqlQuery");
        assertNotClosed();
        this.preparedStatement = connection.prepareStatement(sqlQuery);
        return this;
    }

    public SqlBuilder withParameter(String parameter) throws SQLException {
        assertNotClosed();
        assertPreparedStatement();
        this.preparedStatement.setString(parameterIndex++, parameter);
        return this;
    }

    public SqlBuilder withParameter(boolean parameter) throws SQLException {
        assertNotClosed();
        assertPreparedStatement();
        this.preparedStatement.setBoolean(parameterIndex++, parameter);
        return this;
    }

    public SqlBuilder withParameter(long parameter) throws SQLException {
        assertNotClosed();
        assertPreparedStatement();
        this.preparedStatement.setLong(parameterIndex++, parameter);
        return this;
    }

    public SqlBuilder withParameter(int parameter) throws SQLException {
        assertNotClosed();
        assertPreparedStatement();
        this.preparedStatement.setInt(parameterIndex++, parameter);
        return this;
    }

    /**
     * Closes the PreparedStatement and the ResultSet connected
     * with it. Resets the parameter index so a new
     * PreparedStatement can be created.
     *
     * @return this SqlBuilder instance
     * @throws SQLException in case of any exceptions on the
     * underlying database
     */
    public SqlBuilder continueWith() throws SQLException {
        assertNotClosed();
        this.preparedStatement.close();
        this.preparedStatement = null;
        this.parameterIndex = 1;
        return this;
    }

    public SqlBuilder withTransaction() throws SQLException {
        assertNotClosed();
        this.connection.setAutoCommit(false);
        return this;
    }

    public ResultSet resultSet() throws SQLException {
        assertNotClosed();
        assertPreparedStatement();
        return preparedStatement.executeQuery();
    }

    public SqlBuilder commit() throws SQLException {
        assertNotClosed();
        if (this.connection != null) {
            this.connection.commit();
        }
        return this;
    }

    public void close() {
        isClosed = true;
        if (this.connection != null) {
            try {
                connection.close();
            } catch (SQLException exception) {
                throw new JdbcException("Error closing connection.", exception);
            }
        }
    }

    public void update() throws SQLException {
        assertNotClosed();
        assertPreparedStatement();
        this.preparedStatement.executeUpdate();
    }

    private void assertPreparedStatement() {
        if (preparedStatement == null)
            throw new IllegalStateException("Parameters can only be used with prepared statements");
    }

    private void assertNotClosed() {
        if (isClosed)
            throw new IllegalStateException("This sqlBuilder instance is already closed.");
    }

    private void notNull(Object parameter, String parameterName) {
        if (parameter == null)
            throw new IllegalArgumentException("Parameter " + parameterName + " is mandatory.");
    }
}