/*
 * Copyright (C) 2013, 2014 Brett Wooldridge
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.zaxxer.hikari.pool;

import java.sql.*;

import com.zaxxer.hikari.util.FastList;

/**
 * A factory class that produces proxies around instances of the standard
 * JDBC interfaces.
 *
 *  主要目的就是避免JDK原生的动态代理效率太低了
 *
 *  那往回发问, 为什么数据库操作需要动态代理呢 ?
 *  看github的原文 {@literal
 *  https://github.com/brettwooldridge/HikariCP/issues/1198}
 *
 *  The proxies delegate to the real driver classes. Some proxies,
 *  like the one for ResultSet, only intercept a few methods.
 *  Without the code generation, the proxy would have to implement
 *  all 50+ methods which simply delegate to the wrapped instance.
 *  Code generation, based on reflection, also means that nothing needs
 *  to be done when a new JDK version introduces new JDBC methods to
 *  existing interfaces.
 *
 *  A concrete class is generated from the abstract ProxyConnection class.
 *  Any methods that are not "overridden" by the abstract class, and that
 *  throw SQLException have delegates generated with the following code:
 *
 * {
 *   try {
 *     return delegate.method($$);
 *   } catch (SQLException e) {
 *     throw checkException(e);
 *   }
 * }
 * ... which allows us to inspect the exception to see if it represents
 * a disconnection error.
 * A side-effect is that, yes, the code ends of being flexible with respect to
 * JDBC API changes -- at least all that we have encountered so far.
 *
 *  来自stackoverflow的问题 {@linkral https://stackoverflow.com/questions/52181840/why-generate-hikariproxyconnection-by-javaassist-since-author-already-write-prox}
 *
 *  According to the source code of HikariCP, I found the author generates
 *  HikariProxyConnection by javaassist, but the class do nothing but
 *  invoke the super class method.
 *
 * For example, the HikariProxyConnection's super class is ProxyConnection:
 *
 * public class HikariProxyConnection extends ProxyConnection
 *                      implements AutoCloseable, Connection, Wrapper {
 *    public Statement createStatement() throws SQLException {
 *      try {
 *         return super.createStatement();
 *      } catch (SQLException var2) {
 *         throw this.checkException(var2);
 *      }
 *    }
 *
 *    public PreparedStatement prepareStatement(String var1) throws SQLException {
 *      try {
 *          return super.prepareStatement(var1);
 *      } catch (SQLException var3) {
 *          throw this.checkException(var3);
 *      }
 *    }    }
 * I found ProxyConnection already do a lot of things, HikariProxyConnection
 * only add a try catch block to every method.
 *
 * It would be nice if anyone can give an explanation
 *
 *
 * @author Brett Wooldridge
 */
@SuppressWarnings("unused")
public final class ProxyFactory
{
   private ProxyFactory()
   {
      // unconstructable
   }

   /**
    * Create a proxy for the specified {@link Connection} instance.
    * @param poolEntry the PoolEntry holding pool state
    * @param connection the raw database Connection
    * @param openStatements a reusable list to track open Statement instances
    * @param leakTask the ProxyLeakTask for this connection
    * @param now the current timestamp
    * @param isReadOnly the default readOnly state of the connection
    * @param isAutoCommit the default autoCommit state of the connection
    * @return a proxy that wraps the specified {@link Connection}
    */
   static ProxyConnection getProxyConnection(final PoolEntry poolEntry, final Connection connection, final FastList<Statement> openStatements, final ProxyLeakTask leakTask, final long now, final boolean isReadOnly, final boolean isAutoCommit)
   {
      // Body is replaced (injected) by JavassistProxyFactory
      //注释写着“Body is replaced (injected) by JavassistProxyFactory”，
      // 其实方法body中的代码是在编译时调用JavassistProxyFactory才生成的。
      // 下同
      throw new IllegalStateException("You need to run the CLI build and you need target/classes in your classpath to run.");
   }

   static Statement getProxyStatement(final ProxyConnection connection, final Statement statement)
   {
      // Body is replaced (injected) by JavassistProxyFactory
      throw new IllegalStateException("You need to run the CLI build and you need target/classes in your classpath to run.");
   }

   static CallableStatement getProxyCallableStatement(final ProxyConnection connection, final CallableStatement statement)
   {
      // Body is replaced (injected) by JavassistProxyFactory
      throw new IllegalStateException("You need to run the CLI build and you need target/classes in your classpath to run.");
   }

   static PreparedStatement getProxyPreparedStatement(final ProxyConnection connection, final PreparedStatement statement)
   {
      // Body is replaced (injected) by JavassistProxyFactory
      throw new IllegalStateException("You need to run the CLI build and you need target/classes in your classpath to run.");
   }

   static ResultSet getProxyResultSet(final ProxyConnection connection, final ProxyStatement statement, final ResultSet resultSet)
   {
      // Body is replaced (injected) by JavassistProxyFactory
      throw new IllegalStateException("You need to run the CLI build and you need target/classes in your classpath to run.");
   }

   static DatabaseMetaData getProxyDatabaseMetaData(final ProxyConnection connection, final DatabaseMetaData metaData)
   {
      // Body is replaced (injected) by JavassistProxyFactory
      throw new IllegalStateException("You need to run the CLI build and you need target/classes in your classpath to run.");
   }
}
