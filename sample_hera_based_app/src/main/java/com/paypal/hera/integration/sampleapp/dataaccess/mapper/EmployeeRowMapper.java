package com.paypal.hera.integration.sampleapp.dataaccess.mapper;

import com.paypal.hera.integration.sampleapp.dataaccess.entity.EmployeeEntity;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import java.util.logging.Logger;
import javax.annotation.Generated;
import org.springframework.jdbc.core.RowMapper;

/**
 * EmployeeRowMapper: RowMapper for Employee
 */

public class EmployeeRowMapper implements RowMapper<EmployeeEntity> {

    /**
     * Simple mapping from ResultSet to POJO for database table Employee
     */
    @Override
    public EmployeeEntity mapRow(final ResultSet rs, final int rowNum) throws SQLException {
        final EmployeeEntity employeeEntity = new EmployeeEntity();

        final ResultSetMetaData resultSetMetaData = rs.getMetaData();
        for (int i = 1; i < resultSetMetaData.getColumnCount()+1; i++) {
            switch (resultSetMetaData.getColumnName(i)) {
                case EmployeeEntity.ID:
                    employeeEntity.setId(rs.getInt(EmployeeEntity.ID));
                    break;
                case EmployeeEntity.NAME:
                    employeeEntity.setName(rs.getString(EmployeeEntity.NAME));
                    break;
                case EmployeeEntity.TIME_CREATED:
                    employeeEntity.setTimeCreated(rs.getTimestamp(EmployeeEntity.TIME_CREATED));
                    break;
                case EmployeeEntity.VERSION:
                    employeeEntity.setVersion(rs.getInt(EmployeeEntity.VERSION));
                    break;
                default:
                    String msg = "Column " + resultSetMetaData.getColumnName(i)
                                + " has value in result set, but not defined in Entity/RowMapper. Unable to Map the Column";
                    Logger.getLogger(this.getClass().getSimpleName()).warning(msg);
                    throw new Error(msg);
            }
        }
        return employeeEntity;
    }
}