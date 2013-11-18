package com.github.fluentjdbc;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 * @author Patrick Kranz
 */
@RunWith(MockitoJUnitRunner.class)
public class SqlBuilderTest {

    @Mock
    private DataSource dataSourceMock;

    @Mock
    private Connection connectionMock;

    @Mock
    private PreparedStatement preparedStatementMock;

    private SqlBuilder sqlBuilder;

    @Before
    public void before() throws SQLException {
        when(dataSourceMock.getConnection()).thenReturn(connectionMock);
        when(connectionMock.prepareStatement(any(String.class))).thenReturn(preparedStatementMock);
        sqlBuilder = new SqlBuilder(dataSourceMock);
    }

    @Test
    public void shouldPrepareStatementWhenSqlQueryGiven() throws SQLException {
        sqlBuilder.prepareStatement("select 1 from dual");
        verify(connectionMock, atLeastOnce()).prepareStatement("select 1 from dual");
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenNoSqlQueryGiven() throws SQLException {
        sqlBuilder.prepareStatement(null);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowExceptionWhenClosedSqlBuilderGiven() throws SQLException {
        SqlBuilder sqlBuilder = new SqlBuilder(dataSourceMock);
        sqlBuilder.close();
        sqlBuilder.prepareStatement("some query");
    }

    @Test
    public void shouldAddParameterWhenStringParameterGiven() throws SQLException {
        sqlBuilder.prepareStatement("query").withParameter("test");
        verify(preparedStatementMock, atLeastOnce()).setString(1, "test");
    }

    @Test
    public void shouldAddSecondParameterWithIndexTwoWhenTwoStringParametersGiven() throws SQLException {
        sqlBuilder.prepareStatement("query").withParameter("test").withParameter("second");
        verify(preparedStatementMock, atLeastOnce()).setString(1, "test");
        verify(preparedStatementMock, atLeastOnce()).setString(2, "second");
    }

    @Test
    public void shouldAddSecondParameterWithIndexTwoWhenTwoBooleanParametersGiven() throws SQLException {
        sqlBuilder.prepareStatement("query").withParameter(false).withParameter(true);
        verify(preparedStatementMock, atLeastOnce()).setBoolean(1, false);
        verify(preparedStatementMock, atLeastOnce()).setBoolean(2, true);
    }

    @Test
    public void shouldAddSecondParameterWithIndexTwoWhenTwoIntegerParametersGiven() throws SQLException {
        sqlBuilder.prepareStatement("query").withParameter(1).withParameter(2);
        verify(preparedStatementMock, atLeastOnce()).setInt(1, 1);
        verify(preparedStatementMock, atLeastOnce()).setInt(2, 2);
    }

    @Test
    public void shouldAddSecondParameterWithIndexTwoWhenTwoLongParametersGiven() throws SQLException {
        sqlBuilder.prepareStatement("query").withParameter(1l).withParameter(2l);
        verify(preparedStatementMock, atLeastOnce()).setLong(1, 1l);
        verify(preparedStatementMock, atLeastOnce()).setLong(2, 2l);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowExceptionWhenParameterOnClosedBuilderGiven() throws SQLException {
        sqlBuilder.close();
        sqlBuilder.prepareStatement("bla");
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowExceptionWhenStringParameterWithoutStatementGiven() throws SQLException {
        sqlBuilder.withParameter("test");
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowExceptionWhenBooleanParameterWithoutStatementGiven() throws SQLException {
        sqlBuilder.withParameter(false);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowExceptionWhenIntegerParameterWithoutStatementGiven() throws SQLException {
        sqlBuilder.withParameter(1);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowExceptionWhenLongParameterWithoutStatementGiven() throws SQLException {
        sqlBuilder.withParameter(1L);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowExceptionWhenTransactionOnAlreadyClosedBuilderGiven() throws SQLException {
        sqlBuilder.close();
        sqlBuilder.withTransaction();
    }

    @Test
    public void shouldDisableAutoCommitWhenWithTransactionGiven() throws SQLException {
        sqlBuilder.prepareStatement("select 1 from dual").withTransaction();
        verify(connectionMock, atLeastOnce()).setAutoCommit(false);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowExceptionWhenCommitOnAlreadyClosedBuilderGiven() throws SQLException {
        sqlBuilder.close();
        sqlBuilder.commit();
    }

    @Test
    public void shouldCommitTransactionWhenCommitOnOpenConnectionGiven() throws SQLException {
        sqlBuilder.commit();
    }

   @Test
    public void shouldCloseConnectionWhenOpenConnectionGiven() throws SQLException {
       sqlBuilder.close();
       verify(connectionMock, atLeastOnce()).close();
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowExceptionWhenContinueOnAlreadyClosedBuilderGiven() throws SQLException {
        sqlBuilder.close();
        sqlBuilder.continueWith();
    }

    @Test
    public void shouldCloseStatementWhenContinueWithOnOpenStatementGiven() throws SQLException {
        sqlBuilder.prepareStatement("select 1 from dual");
        sqlBuilder.continueWith();
        verify(preparedStatementMock, times(1)).close();
    }

    @Test
    public void shouldResetParameterIndexWhenContinueWithOnOpenStatementGiven() throws SQLException {
        sqlBuilder.prepareStatement("select 1 from dual");
        sqlBuilder.withParameter(true);
        sqlBuilder.withParameter(false);
        sqlBuilder.continueWith().prepareStatement("select 2 from dual");
        sqlBuilder.withParameter("test");
        verify(preparedStatementMock, times(1)).setBoolean(1, true);
        verify(preparedStatementMock, times(1)).setBoolean(2, false);
        verify(preparedStatementMock, times(1)).setString(1, "test");
    }

    @Test(expected =  IllegalStateException.class)
    public void shouldThrowExceptionWhenResultWithoutStatementGiven() throws SQLException {
        sqlBuilder.resultSet();
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowExceptionWhenResultOnClosedStatementGiven() throws SQLException {
        sqlBuilder.prepareStatement("select 1 from dual");
        sqlBuilder.close();
        sqlBuilder.resultSet();
    }

    @Test
    public void shouldReturnResultSetWhenResultSetOnOpenStatementGiven() throws SQLException {
        sqlBuilder.prepareStatement("select 1 from dual");
        sqlBuilder.resultSet();
        verify(preparedStatementMock, times(1)).executeQuery();
    }

    @Test(expected =  IllegalStateException.class)
    public void shouldThrowExceptionWhenUpdateWithoutStatementGiven() throws SQLException {
        sqlBuilder.update();
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowExceptionWhenUpdateOnClosedStatementGiven() throws SQLException {
        sqlBuilder.prepareStatement("select 1 from dual");
        sqlBuilder.close();
        sqlBuilder.update();
    }

    @Test(expected = JdbcException.class)
    public void shouldThrowRuntimeExceptionWhenSqlExceptionGiven() throws SQLException {
        Mockito.doThrow(new SQLException("something bad happened"))
                .when(connectionMock).close();
        sqlBuilder.prepareStatement("select 1 from dual");
        sqlBuilder.close();
    }

    @Test
    public void shouldReturnResultSetWhenUpdateOnOpenStatementGiven() throws SQLException {
        sqlBuilder.prepareStatement("select 1 from dual");
        sqlBuilder.update();
        verify(preparedStatementMock, times(1)).executeUpdate();
    }
}
