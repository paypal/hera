package com.paypal.integ.odak;

import com.paypal.hera.conf.HeraClientConfigHolder;
import com.paypal.hera.conn.HeraTLSConnectionFactory;
import com.paypal.hera.dal.jdbc.rt.JdbcDriverAdapter;
import com.paypal.hera.dal.jdbc.rt.JdbcDriverAdapterFactory;
import com.paypal.hera.jdbc.HeraDriver;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;

public class OdakDataSource implements javax.sql.DataSource {

    private String dsName = null;

    OdakPool pool;
    OdakPoolManager poolManager = OdakPoolManager.getInstance();


    public String getName() {
        return dsName;
    }

    public OdakDataSource(PoolConfig poolConfig) {
        JdbcDriverAdapter jdbcdriverAdapter = JdbcDriverAdapterFactory.findAdapterForDriver(HeraDriver.class.getName());
        if(jdbcdriverAdapter == null) {
            JdbcDriverAdapterFactory.initAdapter();
        }
        if(poolConfig.isUseSSLConnection()) {
            poolConfig.getConnectionProperties().setProperty(HeraClientConfigHolder.CONNECTION_FACTORY_PROPERTY, HeraTLSConnectionFactory.class.getCanonicalName());
        }
        OdakConfigManager.getInstance().addPoolConfig(poolConfig.getHost(), poolConfig);
        pool = poolManager.createPool(poolConfig.getHost(), new OdakAdapter());
        dsName = poolConfig.getHost();
        OdakPoolManager.getInstance().init(true);
    }


    @Override
    public Connection getConnection() throws SQLException {
        return pool.getConnection();
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Feature not implemented");
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new SQLException("Feature not implemented");
    }
}
