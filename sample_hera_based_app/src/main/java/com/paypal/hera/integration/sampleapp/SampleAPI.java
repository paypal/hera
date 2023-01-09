package com.paypal.hera.integration.sampleapp;

import com.paypal.dal.heramockclient.HERAMockAction;
import com.paypal.dal.heramockclient.HERAMockException;
import com.paypal.dal.heramockclient.HERAMockHelper;

import com.paypal.hera.integration.sampleapp.dataaccess.EmployeeRepository;
import com.paypal.hera.integration.sampleapp.dataaccess.entity.EmployeeEntity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import javax.sql.DataSource;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;


class Input {
    boolean isOdak;
    EmployeeEntity entity;

    public Input(boolean isOdak, EmployeeEntity entity) {
       this.isOdak = isOdak;
       this.entity = entity;
    }
}
@RestController
public class SampleAPI {
    @Autowired
    EmployeeRepository employeeRepository;

    @Autowired
    @Qualifier("heraDataSourceWithHikari")
    DataSource dataSource;

    @Autowired
    @Qualifier("heraDataSourceWithOpenDAK")
    DataSource openDAKDataSource;

    AtomicInteger numberOfQueriesSucceeded = new AtomicInteger();
    AtomicInteger numberOfQueriesFailed = new AtomicInteger();

    ThreadFactory executor = new ThreadFactory() {
        public Thread newThread(Runnable r) {
            Thread t = Executors.defaultThreadFactory().newThread(r);
            t.setDaemon(true);
            t.setName("ParallelTestEnv" + t.getId());
            return t;
        }
    };


    private String basicTest() throws SQLException {

        StringBuilder message = new StringBuilder();
        message.append("Basic Test Result: ");

        Connection dbConn =  SpringJDBCconfig.heraDataSourceWithNoConnPool().getConnection();

        // do standard JDBC
        PreparedStatement pst = dbConn.prepareStatement("select 'abc' from dual");
        ResultSet rs = pst.executeQuery();
        if (rs.next()) {
            message.append("testRead : ").append(rs.getString(1));
        }
        return message.toString();
    }

    private String mockTest() throws HERAMockException {
        EmployeeEntity employee = new EmployeeEntity();
        employee.setId(1);
        employee.setName("mockedResponse");
        employee.setVersion(100);
        HERAMockHelper.addMock("Employee.FIND_BY_ID", employee);
        return employeeRepository.findById(1, false).toString();
    }

    private void delayOnCommit() {
        DataSourceTransactionManager dataSourceTransactionManager = new DataSourceTransactionManager();
        dataSourceTransactionManager.setDataSource(openDAKDataSource);
        DefaultTransactionDefinition definition = new DefaultTransactionDefinition();
        TransactionStatus status = dataSourceTransactionManager.getTransaction(definition);
        try {
            EmployeeEntity employee = new EmployeeEntity();
            employee.setName("mockedResponse");
            employee.setVersion(100);
            employee.setTimeCreated(Timestamp.valueOf(LocalDateTime.now()));
            HERAMockHelper.addMock("Employee.INSERT", "2000" + HERAMockAction.DELAY_ON_COMMIT);
            employeeRepository.insert(employee, true);
            dataSourceTransactionManager.commit(status);
        } finally {
            if(!status.isCompleted())
                dataSourceTransactionManager.rollback(status);
        }
    }

    private void delayOnExec() {
        EmployeeEntity employee = new EmployeeEntity();
        employee.setId(1);
        employee.setName("mockedResponse");
        employee.setVersion(100);
        try {
            HERAMockHelper.addMock("Employee.FIND_BY_ID", employee, 1, 120,
                    false, 2000);
        } catch (HERAMockException e) {
            e.printStackTrace();
        }
        employeeRepository.findById(1, false);

    }

    private void delayOnFetch() {
        EmployeeEntity employee = new EmployeeEntity();
        employee.setId(1);
        employee.setName("mockedResponse");
        employee.setVersion(100);
        HERAMockHelper.addMock("Employee.FIND_BY_NAME", "2000" + HERAMockAction.DELAY_ON_FETCH);

        employeeRepository.findByName("test", false, 50);

    }

    @GetMapping("/basicTest")
    public String simple() {
        StringBuilder message = new StringBuilder();
        try {
            message.append("Basic Test: ").append(basicTest()).append("\n");
            message.append(basicTest()).append("\n");
        }catch (Exception e) {
            message.append("Exception: ").append(e.getMessage()).append("\n");
        }

        return message.toString();
    }

    @GetMapping("/basicMockTest")
    public String basicMockTest() {
        StringBuilder message = new StringBuilder();
        try {
            message.append("Mock Test: ").append(mockTest()).append("\n");
        }catch (Exception e) {
            message.append("Exception: ").append(e.getMessage()).append("\n");
        }
        delayOnCommit();
        delayOnExec();
        delayOnFetch();
        return message.toString();
    }


    @GetMapping("/springJdbcTemplate")
    public String springJdbcTemplate() {
        StringBuilder message = new StringBuilder();
        try {
            message.append("Spring JDBC Template Test result: ").append(employeeRepository.findById(1, false)).append("\n");
        }catch (Exception e) {
            message.append("Exception: ").append(e.getMessage()).append("\n");
        }
        return message.toString();
    }

    @GetMapping("/openDAKTest")
    public String openDAKTest() {
        StringBuilder message = new StringBuilder();
        int totalCalls = 1000;
        int parallel = 25;
        try {
            message.append("Spring JDBC Template Test result: ").append(employeeRepository.findById(1, true)).append("\n");
        }catch (Exception e) {
            message.append("Exception: ").append(e.getMessage()).append("\n");
        }
        callInParallel(totalCalls, parallel, true);
        message.append("Number of request: ").append(totalCalls).append("\nNumber of calls succeeded: ").append(numberOfQueriesSucceeded).append("\nNumber of calls Failed: ").append(numberOfQueriesFailed).append("\n");
        resetStats();
        return message.toString();
    }

    @GetMapping("/hikariTest")
    public String hikariTest() {
        StringBuilder message = new StringBuilder();
        int totalCalls = 1000;
        int parallel = 25;
        try {
            message.append("Spring JDBC Template Test result: ").append(employeeRepository.findById(1, false)).append("\n");
        }catch (Exception e) {
            message.append("Exception: ").append(e.getMessage()).append("\n");
        }
        callInParallel(totalCalls, parallel, false);
        message.append("Number of request: ").append(totalCalls).append("\nNumber of calls succeeded: ").append(numberOfQueriesSucceeded).append("\nNumber of calls Failed: ").append(numberOfQueriesFailed).append("\n");
        resetStats();
        return message.toString();
    }

    void callInParallel(int numOfQueries, int parallel, boolean isOdak) {
        Scheduler scheduler = Schedulers.newParallel(parallel, executor);
        int nextVal = 1;
//        try {
//            nextVal = employeeRepository.maxId() + 1;
//        }catch (Exception ignored) {
//        }
        List<Input> inputs = new ArrayList<>();
        for (int i = nextVal; i<nextVal + numOfQueries; i++) {
            EmployeeEntity employee = new EmployeeEntity();
            employee.setName("test");
            employee.setVersion(1);
            employee.setTimeCreated(Timestamp.valueOf(LocalDateTime.now()));
            inputs.add(new Input(isOdak, employee));
        }
        Disposable task = Flux.fromIterable(inputs).parallel(parallel).runOn(scheduler).subscribe(this::doQuery);
        while ((numberOfQueriesSucceeded.get() + numberOfQueriesFailed.get() < numOfQueries)) {
            try {
                Thread.sleep(3000);
                System.out.println("waiting for queries to be completed " + numOfQueries + " > " + numberOfQueriesSucceeded + " + " + numberOfQueriesFailed);
            } catch (Exception ignored) {
            }
        }
        scheduler.dispose();
        task.dispose();
    }

    private void resetStats() {
        numberOfQueriesFailed.set(0);
        numberOfQueriesSucceeded.set(0);
    }

    private void doQuery(Input input) {
        DataSourceTransactionManager dataSourceTransactionManager = new DataSourceTransactionManager();
        dataSourceTransactionManager.setDataSource(openDAKDataSource);
        DefaultTransactionDefinition definition = new DefaultTransactionDefinition();
        TransactionStatus status = dataSourceTransactionManager.getTransaction(definition);
        Long id = employeeRepository.insert(input.entity, input.isOdak);
        try {
            employeeRepository.findById(id.intValue(), input.isOdak);
            numberOfQueriesSucceeded.addAndGet(1);
            dataSourceTransactionManager.commit(status);
        }catch (Exception e) {
            System.out.println(e.getMessage() + " : " + id + ":" + numberOfQueriesSucceeded.get());
            e.printStackTrace();
            numberOfQueriesFailed.addAndGet(1);
        } finally {
            if(!status.isCompleted())
                dataSourceTransactionManager.rollback(status);
        }
    }
}
