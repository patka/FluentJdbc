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
 * Be sure to call the methods in correct order. Adding a
 * parameter without having a statement will result in an
 * exception. I guess this is obvious but I just want to
 * mention it :)
 *
 * @author Patrick Kranz
 */
public class SqlBuilder {
    private final Connection connection;
    private PreparedStatement preparedStatement;
    private int parameterIndex;
    private boolean isClosed;

    /**
     * Creates a new instance and retrieves a connection from the
     * given dataSource.
     *
     * @param dataSource the dataSource that is associated with the
     *                   database. May not bet null.
     * @throws SQLException if an error happens when opening the
     *                      connection
     */
    public SqlBuilder(DataSource dataSource) throws SQLException {
        notNull(dataSource, "dataSource");
        this.connection = dataSource.getConnection();
        this.parameterIndex = 1;
        this.isClosed = false;
    }

    /**
     * Creates a <code>PreparedStatement</code> that will be used until
     * <code>close</code> or <code>continueWith</code> is called.
     *
     * @param sqlQuery the statement that will be executed.
     *                 May not be null.
     * @return this current sqlBuilder instance
     * @throws SQLException if an exception on the underlying connection
     *                      happens
     */
    public SqlBuilder prepareStatement(String sqlQuery) throws SQLException {
        notNull(sqlQuery, "sqlQuery");
        assertNotClosed();
        this.preparedStatement = connection.prepareStatement(sqlQuery);
        return this;
    }

    /**
     * Adds a String parameter on the next position of the query on the
     * underlying <code>PreparedStatement</code>. SqlBuilder manages an
     * internal index counter. So the first call to a <code>withParameter</code>
     * method will set the index 1, the second call will set index 2 and so on.
     *
     * @param parameter the parameter of the query
     * @return this current sqlBuilder instance
     * @throws SQLException if an exception happens when setting the parameter
     */
    public SqlBuilder withParameter(String parameter) throws SQLException {
        assertNotClosed();
        assertPreparedStatement();
        this.preparedStatement.setString(parameterIndex++, parameter);
        return this;
    }

    /**
     * Adds a boolean parameter on the next position of the query on the
     * underlying <code>PreparedStatement</code>. SqlBuilder manages an
     * internal index counter. So the first call to a <code>withParameter</code>
     * method will set the index 1, the second call will set index 2 and so on.
     *
     * @param parameter the parameter of the query
     * @return this current sqlBuilder instance
     * @throws SQLException if an exception happens when setting the parameter
     */
    public SqlBuilder withParameter(boolean parameter) throws SQLException {
        assertNotClosed();
        assertPreparedStatement();
        this.preparedStatement.setBoolean(parameterIndex++, parameter);
        return this;
    }

    /**
     * Adds a long parameter on the next position of the query on the
     * underlying <code>PreparedStatement</code>. SqlBuilder manages an
     * internal index counter. So the first call to a <code>withParameter</code>
     * method will set the index 1, the second call will set index 2 and so on.
     *
     * @param parameter the parameter of the query
     * @return this current sqlBuilder instance
     * @throws SQLException if an exception happens when setting the parameter
     */
    public SqlBuilder withParameter(long parameter) throws SQLException {
        assertNotClosed();
        assertPreparedStatement();
        this.preparedStatement.setLong(parameterIndex++, parameter);
        return this;
    }

    /**
     * Adds an int parameter on the next position of the query on the
     * underlying <code>PreparedStatement</code>. SqlBuilder manages an
     * internal index counter. So the first call to a <code>withParameter</code>
     * method will set the index 1, the second call will set index 2 and so on.
     *
     * @param parameter the parameter of the query
     * @return this current sqlBuilder instance
     * @throws SQLException if an exception happens when setting the parameter
     */
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
     * @return this sqlBuilder instance
     * @throws SQLException in case of any exception on the
     * underlying database
     */
    public SqlBuilder continueWith() throws SQLException {
        assertNotClosed();
        this.preparedStatement.close();
        this.preparedStatement = null;
        this.parameterIndex = 1;
        return this;
    }

    /**
     * Turns the auto commit on the connection of. Everything that
     * happens after a call to this method will be in one transaction.
     *
     * @return this sqlBuilder instance
     * @throws SQLException in case of any exception on the connection
     */
    public SqlBuilder withTransaction() throws SQLException {
        assertNotClosed();
        this.connection.setAutoCommit(false);
        return this;
    }

    /**
     * Sets auto commit on the underlying connection to true. Every
     * statement that will be executed will be committed immediately.
     *
     * This is the default setting according to the JDBC specification.
     * Therefore, this is onlyl required if <code>withTransaction</code>
     * was invoked previously.
     *
     * @return this sqlBuilder instance
     * @throws SQLException in case of any exception on the connection
     */
    public SqlBuilder withoutTransaction() throws SQLException {
        assertNotClosed();
        this.connection.setAutoCommit(true);
        return this;
    }

    /**
     * Executes the underlying <code>PreparedStatement</code> as a query
     *
     * @return the ResultSet containing the result
     * @throws SQLException in case of any exception while executing the query
     */
    public ResultSet resultSet() throws SQLException {
        assertNotClosed();
        assertPreparedStatement();
        return preparedStatement.executeQuery();
    }

    /**
     * Rolls back the current transaction. Only has effect if
     * <code>withTransaction</code> was invoked.
     *
     * @return this sqlBuilder instance
     * @throws SQLException in case of any exception on the connection
     */
    public SqlBuilder rollback() throws SQLException {
        assertNotClosed();
        connection.rollback();
        return this;
    }

    /**
     * Commits the current transaction. Only has effect if
     * <code>withTransaction</code> was invoked.
     *
     * @return this sqlBuilder instance
     * @throws SQLException in case of any exception on the connection
     */
    public SqlBuilder commit() throws SQLException {
        assertNotClosed();
        if (this.connection != null) {
            this.connection.commit();
        }
        return this;
    }

    /**
     * This will close the underlying connection and therefore
     * all resources depending on it, e.g. <code>PreparedStatement</code>
     *
     * The internal state of this sqlBuilder instance will be closed. Any attempt
     * to call any method on this instance except close will result in an exception.
     *
     * This method also wrappes a possible <code>SQLException</code> to an unchecked
     * JdbcException.
     *
     * @throws JdbcException in case the close method throws a <code>SQLException</code>.
     */
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

    /**
     * Executes the <code>PreparedStatement</code> as an update.
     *
     * @return the number of rows affected
     * @throws SQLException in case of any exception on update execution‚
     */
    public int update() throws SQLException {
        assertNotClosed();
        assertPreparedStatement();
        return this.preparedStatement.executeUpdate();
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