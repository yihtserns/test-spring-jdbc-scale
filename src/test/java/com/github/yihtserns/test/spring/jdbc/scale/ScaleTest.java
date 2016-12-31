package com.github.yihtserns.test.spring.jdbc.scale;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.SqlOutParameter;
import org.springframework.jdbc.core.SqlParameter;
import org.springframework.jdbc.core.simple.SimpleJdbcCall;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.Types;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * @author yihtserns
 */
public class ScaleTest {

    private static final String DB_NAME = "memory:derbyDB";
    private static final String JDBC_URL = "jdbc:derby:" + DB_NAME;

    private JdbcTemplate jdbcTemplate;

    @Before
    public void startEmbeddedDerby() throws SQLException {
        Connection connection = DriverManager.getConnection(JDBC_URL + ";create=true");

        jdbcTemplate = new JdbcTemplate(new SingleConnectionDataSource(connection, false));
        createStoredProcedure();
    }

    @After
    public void shutdownEmbeddedDerby() {
        try {
            DriverManager.getConnection(JDBC_URL + ";drop=true");
        } catch (SQLException ex) {
            assertThat(ex.getMessage(), is("Database '" + DB_NAME + "' dropped."));
        }
    }

    @Test
    public void shouldPadWhenDecimalPointIsLessThanScale() {
        BigDecimal result = callStoredProcedure(new BigDecimal(3.66));

        assertThat(result, is(new BigDecimal("3.66000")));
    }

    @Test
    public void shouldTruncateWhenDecimalPointIsMoreThanScale() {
        BigDecimal result = callStoredProcedure(new BigDecimal(3.66666666));

        assertThat(result, is(new BigDecimal("3.66666")));
    }

    @Test
    public void passingDoubleAsInputWouldHaveInaccurateResult() {
        BigDecimal result = callStoredProcedure(3.66666);

        assertThat(result, is(new BigDecimal("3.00000")));
    }

    private void createStoredProcedure() {
        String sql = "CREATE PROCEDURE IDENTITY_PROC(IN INVAL DECIMAL(10, 5), OUT OUTVAL DECIMAL(10, 5))" +
                " PARAMETER STYLE JAVA READS SQL DATA" +
                " LANGUAGE JAVA" +
                " EXTERNAL NAME 'com.github.yihtserns.test.spring.jdbc.scale.ScaleTest.identity'";
        jdbcTemplate.execute(sql);
    }

    private <T> T callStoredProcedure(Object... args) {
        Map<String, Object> result = new SimpleJdbcCall(jdbcTemplate)
                .withProcedureName("IDENTITY_PROC")
                .execute(args);

        return (T) result.get("OUTVAL");
    }

    public static void identity(BigDecimal inval, BigDecimal[] outval) {
        outval[0] = inval;
    }
}
