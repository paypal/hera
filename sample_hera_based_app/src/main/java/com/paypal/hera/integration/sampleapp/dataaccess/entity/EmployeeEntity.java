package com.paypal.hera.integration.sampleapp.dataaccess.entity;

import java.sql.Timestamp;
import javax.annotation.Generated;

/**
 * EmployeeEntity: POJO for database table Employee
 */

public class EmployeeEntity {

    /**
     * ColumnName: ID, 
     * ColumnType: INT (10)
     */
    public static final String ID = "ID";

    /**
     * ColumnName: NAME, 
     * ColumnType: VARCHAR (100)
     */
    public static final String NAME = "NAME";

    /**
     * ColumnName: TIME_CREATED, 
     * ColumnType: TIMESTAMP (0)
     */
    public static final String TIME_CREATED = "TIME_CREATED";

    /**
     * ColumnName: VERSION, 
     * ColumnType: INT (10)
     */
    public static final String VERSION = "VERSION";

    /**
     * ColumnType: INT (10)
     */
    private Integer id;

    /**
     * ColumnType: VARCHAR (100)
     */
    private String name;

    /**
     * ColumnType: TIMESTAMP (0)
     */
    private Timestamp timeCreated;

    /**
     * ColumnType: INT (10)
     */
    private Integer version;

    /**
     * Getter method for ID
     * @return id pojo variable to get
     */
    public Integer getId() {
        return id;
    }

    /**
     * Setter method for ID
     * @param id pojo variable to set
     */
    public void setId(final Integer id) {
        this.id = id;
    }

    /**
     * Getter method for NAME
     * @return name pojo variable to get
     */
    public String getName() {
        return name;
    }

    /**
     * Setter method for NAME
     * @param name pojo variable to set
     */
    public void setName(final String name) {
        this.name = name;
    }

    /**
     * Getter method for TIME_CREATED
     * @return timeCreated pojo variable to get
     */
    public Timestamp getTimeCreated() {
        return timeCreated;
    }

    /**
     * Setter method for TIME_CREATED
     * @param timeCreated pojo variable to set
     */
    public void setTimeCreated(final Timestamp timeCreated) {
        this.timeCreated = timeCreated;
    }

    /**
     * Getter method for VERSION
     * @return version pojo variable to get
     */
    public Integer getVersion() {
        return version;
    }

    /**
     * Setter method for VERSION
     * @param version pojo variable to set
     */
    public void setVersion(final Integer version) {
        this.version = version;
    }

    /**
     * toString Method
     * @return String representation of the entity
     */
    @Override
    public String toString() {
        return 
                "ID: " + this.id + ", " + 
                "NAME: " + this.name + ", " + 
                "TIME_CREATED: " + this.timeCreated + ", " + 
                "VERSION: " + this.version;
    }
}