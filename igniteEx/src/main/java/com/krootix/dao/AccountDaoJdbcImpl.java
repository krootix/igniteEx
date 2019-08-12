package com.krootix.dao;

import com.krootix.app;
import com.krootix.entity.Account;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AccountDaoJdbcImpl implements AccountDao {
    public static final String LOAD_SQL = "SELECT id, name, balance, lastoperation, upd_date FROM account WHERE id = ?";
    //public static final String LOAD_ALL_SQL = "SELECT id, name, balance, lastoperation, upd_date FROM account";
    public static final String LOAD_ALL_SQL = "SELECT * FROM account";

    public static final String DROP_TABLE_ACCOUNT = "DROP TABLE IF EXISTS account";
    public static final String CREATE_TABLE_ACCOUNT =
            "CREATE TABLE if not exists `my_db`.`account` (\n" +
            "  `id` INT NOT NULL,\n" +
            "  `name` VARCHAR(45) NOT NULL,\n" +
            "  `balance` DOUBLE NOT NULL,\n" +
            "  `lastoperation` DOUBLE NOT NULL,\n" +
            "  `upd_date` BIGINT(19) NOT NULL,\n" +
            "  PRIMARY KEY (`id`));";
    public static final String INSERT_INTO_TABLE = "insert_into_table_account.sql";

    public static final String JDBC_URL = "jdbc:mysql://127.0.0.1:3306/my_db?serverTimezone=UTC&useSSL=false";
    public static final String LOGIN = "root";
    public static final String PASSWORD = "root";
    public static Logger LOGGER = LoggerFactory.getLogger(AccountDaoJdbcImpl.class);

    public void createDbAccount() throws IOException, URISyntaxException {
        PreparedStatement ps = null;
        try (Connection conn = connection()) {
            conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            //conn.setAutoCommit(false);
            ps = conn.prepareStatement(DROP_TABLE_ACCOUNT);
            ps.execute();
            ps = conn.prepareStatement(CREATE_TABLE_ACCOUNT);
            ps.execute();
            String data = readSQL(INSERT_INTO_TABLE);
            for (String retval : data.split(";")) {
                ps.execute(retval + ";");
            }
            //ps = conn.prepareStatement(readSQL(INSERT_INTO_TABLE));
            //ps.executeUpdate();
            //conn.commit();
            LOGGER.info("Table of accounts has been created");
            //conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private String readSQL(String file) throws URISyntaxException, IOException {
        Class clazz = app.class;
        Path path = Paths.get(clazz.getClassLoader()
                .getResource(file).toURI());

        Stream<String> lines = Files.lines(path);
        String data = lines.collect(Collectors.joining("\n"));
        lines.close();
        return data;
    }

    public List<Account> selectAll() {
        PreparedStatement ps = null;
        ResultSet rs = null;
        List<Account> accounts = new ArrayList<Account>();
        //Connection conn = null;
        try (Connection conn = connection()) {
            conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            conn.setAutoCommit(false);
            ps = conn.prepareStatement(LOAD_ALL_SQL);
            rs = ps.executeQuery();
            while (rs.next()) {
                Account account = new Account(rs.getLong(1),
                        rs.getString(2),
                        rs.getDouble(3),
                        rs.getDouble(4),
                        rs.getLong(5));
                accounts.add(account);
            }
            //conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return accounts;
    }

    private Connection connection() throws SQLException {
        return openConnection(true);
    }

    private Connection openConnection(boolean autocommit) throws SQLException {
        //Открытое соединение с системами RDBMS (Oracle, MySQL, Postgres, DB2, Microsoft SQL и т. Д.)
        //В этом примере мы используем базу данных Oracle.
        //Locale.setDefault(Locale.ENGLISH);
        Connection conn = DriverManager.getConnection(JDBC_URL, LOGIN, PASSWORD);
        conn.setAutoCommit(autocommit);
        return conn;
    }
}
