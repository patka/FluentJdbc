FluentJdbc  [![Build Status](https://travis-ci.org/patka/FluentJdbc.png)](https://travis-ci.org/patka/FluentJdbc)
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
  if (sqlBuilder != null) {
    sqlBuilder.close();
}
```
