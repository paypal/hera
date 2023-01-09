package com.paypal.hera.integration.sampleapp.dataaccess.repositoryimpl;

import com.paypal.hera.integration.sampleapp.dataaccess.entity.EmployeeEntity;
import com.paypal.hera.integration.sampleapp.dataaccess.mapper.EmployeeRowMapper;
import com.paypal.hera.integration.sampleapp.dataaccess.EmployeeRepository;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Component;

import java.util.List;


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

    private DataSource dataSource;

    private JdbcTemplate odakJdbcTemplate;

    public DataSource getDataSource() {
        return dataSource;
    }



    /**
     * namedJdbcTemplate used to query - bind keys are named
     */
    NamedParameterJdbcTemplate namedJdbcTemplate;

    NamedParameterJdbcTemplate odakNamedJdbcTemplate;

    @Autowired
    public EmployeeRepositoryImpl(@Qualifier(value = "heraDataSourceWithHikari") DataSource dataSource,
                                  @Qualifier(value = "heraDataSourceWithOpenDAK") DataSource odakDataSource) {
        this.dataSource = dataSource;
        jdbcTemplate = new JdbcTemplate(dataSource);
        odakJdbcTemplate = new JdbcTemplate(odakDataSource);
    }

    @PostConstruct
    /**
     * namedJdbcTemplate gets initialized at this object post-construct
     */
    void createNamedJdbcTemplate() {
        namedJdbcTemplate = new NamedParameterJdbcTemplate(jdbcTemplate);
        odakNamedJdbcTemplate = new NamedParameterJdbcTemplate(odakJdbcTemplate);
    }

    /**
     * Implementation for Find by ID returns one EmployeeEntity
     *
     * @param id where constraint for SQL
     * @return EmployeeEntity
     */
    @Override
    public EmployeeEntity findById(final Integer id, boolean odak) {

        final MapSqlParameterSource mapSqlParameterSource = new MapSqlParameterSource()
        .addValue("id", id);

        if (odak)
            return odakNamedJdbcTemplate.queryForObject(EmployeeQueries.FIND_BY_ID,
                    mapSqlParameterSource, new EmployeeRowMapper());
        return namedJdbcTemplate.queryForObject(EmployeeQueries.FIND_BY_ID, 
            mapSqlParameterSource, new EmployeeRowMapper());
    }

    @Override
    public List<EmployeeEntity> findByName(String name, boolean odak, int fetchSize) {
        final MapSqlParameterSource mapSqlParameterSource = new MapSqlParameterSource()
                .addValue("name", name);
        if (odak) {
            odakJdbcTemplate.setFetchSize(fetchSize);
            return new NamedParameterJdbcTemplate(odakJdbcTemplate).query(EmployeeQueries.FIND_BY_NAME,
                    mapSqlParameterSource, new EmployeeRowMapper());
        }
        jdbcTemplate.setFetchSize(fetchSize);
        return new NamedParameterJdbcTemplate(jdbcTemplate).query(EmployeeQueries.FIND_BY_NAME,
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
    public Long insert(final EmployeeEntity employee, final boolean odak) {

        final MapSqlParameterSource mapSqlParameterSource = new MapSqlParameterSource()
            .addValue("name", employee.getName())
            .addValue("timeCreated", employee.getTimeCreated())
            .addValue("version", employee.getVersion());
        KeyHolder keyHolder = new GeneratedKeyHolder();
        if (odak) {
            odakNamedJdbcTemplate.update(EmployeeQueries.INSERT, mapSqlParameterSource, keyHolder);
            return (Long) keyHolder.getKey();
        }
        namedJdbcTemplate.update(EmployeeQueries.INSERT, mapSqlParameterSource, keyHolder);
        return (Long) keyHolder.getKey();
    }

    @Override
    public Integer maxId() {

        final MapSqlParameterSource mapSqlParameterSource = new MapSqlParameterSource();

        return odakNamedJdbcTemplate.queryForObject(EmployeeQueries.FIND_MAX_ID,
                mapSqlParameterSource, Integer.class);
    }


}