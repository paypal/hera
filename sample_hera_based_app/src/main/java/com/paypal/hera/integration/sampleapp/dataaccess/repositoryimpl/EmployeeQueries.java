package com.paypal.hera.integration.sampleapp.dataaccess.repositoryimpl;

import javax.annotation.Generated;

/**
 * Queries related to Table employee 
 **/
/**
 * Auto generated queries for database table Employee
 */
public class EmployeeQueries {
    /**
     * Employee.FIND_BY_ID
     */
    public static final String FIND_BY_ID = "SELECT /* Employee.FIND_BY_ID */ " + 
            "ID, NAME, TIME_CREATED, VERSION " + 
            "FROM employee " +
            "where ID= :id";

    public static final String FIND_BY_NAME = "SELECT /* Employee.FIND_BY_NAME */ " +
            "ID, NAME, TIME_CREATED, VERSION " +
            "FROM employee " +
            "where NAME= :name";


    public static final String FIND_MAX_ID = "SELECT /* Employee.FIND_MAX_ID */ " +
            "MAX(ID) " +
            "FROM employee";

    /**
     * Employee.UPDATE_BY_ID
     */
    public static final String UPDATE_BY_ID = "UPDATE /* Employee.UPDATE_BY_ID */ employee SET " + 
            "ID = :id, NAME = :name, TIME_CREATED = :timeCreated, VERSION = :version" + 
            " where ID= :id";

    /**
     * Employee.INSERT
     */
    public static final String INSERT = "INSERT /* Employee.INSERT */ INTO employee " + 
            "(NAME, TIME_CREATED, VERSION) " +
            "VALUES (:name, :timeCreated, :version)";
}