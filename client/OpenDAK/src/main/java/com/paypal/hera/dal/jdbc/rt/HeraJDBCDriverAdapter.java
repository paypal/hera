package com.paypal.hera.dal.jdbc.rt;


import com.paypal.hera.ex.HeraIOException;
import com.paypal.hera.ex.HeraInternalErrorException;

import java.io.ByteArrayInputStream;
import java.io.StringReader;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;

public class HeraJDBCDriverAdapter implements JdbcDriverAdapter{

    @Override
    public void setStringParameter(PreparedStatement stmt, int index, String value, boolean isUtf8Db) throws SQLException {
        if (value == null || value.length() < 4000) {
            stmt.setString(index, value);
            return;
        }

        StringReader sread = new StringReader(value);
        stmt.setCharacterStream(index,	sread, value.length());
    }

    @Override
    public void setBytesParameter(PreparedStatement stmt, int index, byte[] value) throws SQLException {
        // Oracle limitation in driver, cannot use over 2000 bytes with
        // setBytes method until 9.0.1 driver
        // We have to do this or we fill up the share memory space on the
        // database with duplicate child statements because of oracle
        // driver limitation (a.k.a. bug).
        // This is documented in JDBC 8.1.7 manual section 3-30.
        if (value == null) {
            stmt.setBytes(index, null);
            return;
        }

        if (value.length < 2000) {
            stmt.setBytes(index, value);
            return;
        }

        // If you get an exception from batch because we are using
        // a stream, which is not compatible with batched statement, you
        // cannot use batch with your statement, because we have
        // no way of setting the bytes on a batch statement
        // without causing the above problem when the length
        // of the bytes is > 2000 until the 9.0.1 driver is used.
        // The use of the binary stream is the oracle suggested
        // work around to the 2000 length limitation bug.
        ByteArrayInputStream bais = new ByteArrayInputStream(value);
        stmt.setBinaryStream(index, bais, value.length);
    }

    @Override
    public boolean supportsTransactionIsolation() {
        return true;
    }

    @Override
    public boolean expectsRetryOnConnectIoException(boolean forceRetryOnIoException) {
        return false;
    }

    @Override
    public boolean shouldCausePoolFlush(SQLException sqlException) {
        //cause markdown
        return sqlException instanceof HeraIOException || sqlException instanceof HeraInternalErrorException;
    }

    @Override
    public ArrayList getDbSessionParameterList(HashMap dbSessionParameters) {
        return null;
    }

    @Override
    public void setTimestampParameter(PreparedStatement stmt, int index, Timestamp value) throws SQLException {
        stmt.setTimestamp(index, value);

    }
}
