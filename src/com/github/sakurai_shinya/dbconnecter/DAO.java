package com.github.sakurai_shinya.dbconnecter;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

public class DAO implements AutoCloseable {

	private Connection conn;

	public DAO(String url, Properties propaties, TransactionIsolation ti) throws SQLException, ClassNotFoundException {
		Class.forName(propaties.getProperty("driver"));
		this.conn = DriverManager.getConnection(url, propaties);
		switch (ti) {
		case NONE:
			this.conn.setTransactionIsolation(Connection.TRANSACTION_NONE);
			break;
		case READ_UNCOMMITTED:
			this.conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
			break;
		case READ_COMMITTED:
			this.conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
			break;
		case REPEATABLE_READ:
			this.conn.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
			break;
		case SERIALIZABLE:
			this.conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
			break;
		}
		this.conn.setAutoCommit(false);
	}

	@Override
	public void close() throws SQLException {
		if ((this.conn != null) && !this.conn.isClosed()) {
			this.conn.commit();
			this.conn.close();
			this.conn = null;
		}
	}

	public void rollback() throws SQLException {
		if ((this.conn != null) && !this.conn.isClosed()) {
			this.conn.rollback();
		}
	}

	public PreparedStatement getStatement(String sql) throws SQLException {
		if ((this.conn != null) && !this.conn.isClosed()) {
			return this.conn.prepareStatement(sql);
		}
		return null;
	}
}
