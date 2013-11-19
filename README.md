FluentJdbc  [![Build Status](https://travis-ci.org/patka/FluentJdbc.png)](https://travis-ci.org/patka/FluentJdbc) [![Coverage Status][![Coverage Status](https://coveralls.io/repos/patka/FluentJdbc/badge.png?branch=master)](https://coveralls.io/r/patka/FluentJdbc?branch=master)
==========

FluentJdbc is a very simple wrapper around the JDBC interface. It is a very small
and simple project. The goal is to make life a little bit easier
when working with plain JDBC without introducing a new "framework".

With FluentJdbc you can write code like this:
```
SqlBuilder sqlBuilder = null
try {
  ResultSet resultSet = sqlBuilder
    .prepareStatement("select username from User where id = ?")
    .withParameter(3l)
    .resultSet();
} catch (SqlException exception) {
  // do something useful here
} finally {
    sqlBuilder.close();
}
```

In case you want to work with transactions, you can do it like this:
```
SqlBuilder sqlBuilder = null
try {
  ResultSet resultSet = sqlBuilder
    .withTransaction()
    .prepareStatement("select username from User where id = ?")
    .withParameter(3l)
    .resultSet();
  // do something here
  sqlBuilder.continueWith()
    .prepareStatement("update User set username = ? where id = ?")
    .withParameter("test")
    .withParameter(3l)
    .update();
  sqlBuilder.commit()
} catch (SqlException exception) {
  // do something useful here
} finally {
    sqlBuilder.close();
}
```

