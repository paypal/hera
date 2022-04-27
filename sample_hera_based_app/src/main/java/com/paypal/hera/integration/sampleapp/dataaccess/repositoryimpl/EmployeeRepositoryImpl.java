package com.paypal.hera.integration.sampleapp.dataaccess.repositoryimpl;

import com.paypal.hera.integration.sampleapp.dataaccess.entity.EmployeeEntity;
import com.paypal.hera.integration.sampleapp.dataaccess.mapper.EmployeeRowMapper;
import com.paypal.hera.integration.sampleapp.dataaccess.EmployeeRepository;

import javax.annotation.Generated;
import javax.annotation.PostConstruct;
import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;


/**
 * EmployeeRepositoryImpl: Repository implementation for application to talk to database layer.
 */

@Component
public class EmployeeRepositoryImpl implements EmployeeRepository {

    /**
     * dsimport should have data-source name="local"
     * This bean gets injected during dal initialization
     */
    private JdbcTemplate jdbcTemplate;

    /**
     * namedJdbcTemplate used to query - bind keys are named
     */
    NamedParameterJdbcTemplate namedJdbcTemplate;

    @Autowired
    public EmployeeRepositoryImpl(@Qualifier(value = "mysqlHikariDataSource") DataSource dataSource) {
        jdbcTemplate = new JdbcTemplate(dataSource);
    }

    @PostConstruct
    /**
     * namedJdbcTemplate gets initialized at this object post-construct
     */
    void createNamedJdbcTemplate() {
        namedJdbcTemplate = new NamedParameterJdbcTemplate(jdbcTemplate);
    }

    /**
     * Implementation for Find by ID returns one EmployeeEntity
     *
     * @param id where constraint for SQL
     * @return EmployeeEntity
     */
    @Override
    public EmployeeEntity findById(final Integer id) {

        final MapSqlParameterSource mapSqlParameterSource = new MapSqlParameterSource()
        .addValue("id", id);

        return namedJdbcTemplate.queryForObject(EmployeeQueries.FIND_BY_ID, 
            mapSqlParameterSource, new EmployeeRowMapper());
    }


    /**
     * implementation for update including all columns
     *
     * @return number of rows affected
     * @param employee bind in value for update
     */
    @Override
    public int updateById(final EmployeeEntity employee) {

        final MapSqlParameterSource mapSqlParameterSource = new MapSqlParameterSource()
                .addValue("id", employee.getId())
                .addValue("name", employee.getName())
                .addValue("timeCreated", employee.getTimeCreated())
                .addValue("version", employee.getVersion());

        return namedJdbcTemplate.update(EmployeeQueries.UPDATE_BY_ID, 
            mapSqlParameterSource);
    }

    /**
     * implementation for insert including all columns
     * @param employee bind in value for insert
     *
     * @return number of rows inserted
     */
    @Override
    public int insert(final EmployeeEntity employee) {

        final MapSqlParameterSource mapSqlParameterSource = new MapSqlParameterSource()
            .addValue("id", employee.getId())
            .addValue("name", employee.getName())
            .addValue("timeCreated", employee.getTimeCreated())
            .addValue("version", employee.getVersion());

        return namedJdbcTemplate.update(EmployeeQueries.INSERT, mapSqlParameterSource);
    }


}