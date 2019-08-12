package com.krootix;

import com.krootix.entity.Account;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreSession;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.resources.CacheStoreSessionResource;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.cache.Cache;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import java.sql.*;
import java.util.*;

public class CacheJdbcAccountStore implements CacheStore<Long, Account> {
    //private final ConnectionFactory factory = new ConnectionFactoryJdbc();
    public static final String LOAD_SQL = "SELECT id, name, balance, lastoperation, upd_date FROM account WHERE id = ?";
    public static final String SAVE_SQL = "UPDATE account SET balance = ?, lastoperation = ?, upd_date = ? WHERE id = ?";
    public static final String DELETE_SQL = "DELETE FROM account WHERE id = ?";
    //public static final String LOAD_ALL_SQL = "SELECT id, name, balance, lastoperation, upd_date FROM account";
    public static final String LOAD_ALL_SQL = "SELECT * FROM account";

    public static final String JDBC_URL = "jdbc:mysql://127.0.0.1:3306/my_db?serverTimezone=UTC&useSSL=false";
    public static final String LOGIN = "root";
    public static final String PASSWORD = "root";
    public static Logger LOGGER = LoggerFactory.getLogger(CacheJdbcAccountStore.class);

    @CacheStoreSessionResource
    private CacheStoreSession session;

    @Override
    public void loadCache(IgniteBiInClosure<Long, Account> igniteBiInClosure, @Nullable Object... args) throws CacheLoaderException {
        if (args == null || args.length == 0 || args[0] == null)
            throw new CacheLoaderException("Expected entry count parameter is not provided.");
        final int entryCnt = (Integer) args[0];
        try (Connection conn = connection()) {

            try (PreparedStatement st = conn.prepareStatement(LOAD_ALL_SQL)) {
                try (ResultSet rs = st.executeQuery()) {
                    int cnt = 0;
                    while (cnt < entryCnt && rs.next()) {
                        Account account = new Account(
                                rs.getLong(1),
                                rs.getString(2),
                                rs.getDouble(3),
                                rs.getDouble(4),
                                rs.getLong(5));
                        igniteBiInClosure.apply(account.getId(), account);
                        cnt++;
                        System.out.println(account.toString());
                    }
                }
            }
        } catch (SQLException e) {
            throw new CacheLoaderException("Failed to load values from cache store.", e);
        }
    }

    // Complete transaction or simply close connection if there is no transaction.

    @Override
    public Account load(Long key) /*throws CacheLoaderException*/ {
        //LOGGER.debug("connection %d: obtained", conn);
        PreparedStatement ps = null;
        ResultSet rs = null;
        //Account account = new Account();

        try (Connection conn = connection()) {

            ps = conn.prepareStatement(LOAD_SQL);
            ps.setLong(1, key);
            rs = ps.executeQuery();
            //LOGGER.info("account [" + key + "] has been found");
            return rs.next() ? new Account(rs.getLong(1),
                    rs.getString(2),
                    rs.getDouble(3),
                    rs.getDouble(4),
                    rs.getLong(5)
            ) : null;

            //conn.commit();
        } catch (SQLException /*| ClassNotFoundException*/ e) {
            throw new CacheLoaderException("Failed to load: " + key, e);
            //e.printStackTrace();
        }
    }

    @Override
    public Map<Long, Account> loadAll(Iterable<? extends Long> keys) throws CacheLoaderException {
        try (Connection conn = connection()) {
            try (PreparedStatement st = conn.prepareStatement(
                    LOAD_ALL_SQL)) {
                Map<Long, Account> loaded = new HashMap<>();

                for (Long key : keys) {
                    st.setLong(1, key);

                    try (ResultSet rs = st.executeQuery()) {
                        if (rs.next())
                            loaded.put(key, new Account(rs.getLong(1),
                                    rs.getString(2),
                                    rs.getDouble(3),
                                    rs.getDouble(4),
                                    rs.getLong(5)
                            ));
                    }
                }
                return loaded;
            }
        } catch (SQLException e) {
            throw new CacheLoaderException("Failed to loadAll: " + keys, e);
        }
    }

    @Override
    public void write(Cache.Entry<? extends Long, ? extends Account> entry) /*throws CacheWriterException*/ {
        Long key = entry.getKey();
        Account val = entry.getValue();
        //Connection conn = getConnection();
        //LOGGER.debug("connection %d: obtained", conn);
        //PreparedStatement ps = null;
        ResultSet rs = null;
        try (Connection conn = connection()) {

            try (PreparedStatement ps = conn.prepareStatement(SAVE_SQL)) {
                ps.setDouble(1, val.getBalance());
                ps.setDouble(2, val.getLastOperation());
                //java.sql.Date d = new java.sql.Date(val.getUpd_date().getTime());
                ps.setLong(3, val.getUpd_date()); // val.getUpd_date());
                ps.setLong(4, val.getId());
                int isSucess = ps.executeUpdate();
                //ps.close();
                //conn.commit();
            }
            //"insert into KLADR (id, code, name, upd_date) values (?, ?, ?, ?)")) {

        } catch (SQLException e) {
            throw new CacheWriterException("Failed to write [key=" + key + ", val=" + val + ']', e);
        }
    }

    @Override
    public void writeAll(Collection<Cache.Entry<? extends Long, ? extends Account>> entries) throws CacheWriterException {
        //public static final String SAVE_SQL =
        // "UPDATE account SET balance = ?, lastoperation = ?, upd_date = ? WHERE id = ?";
        try (Connection conn = connection()) {
            // Syntax of MERGE statement is database specific and should be adopted for your database.
            // If your database does not support MERGE statement then use sequentially update, insert statements.
            try (PreparedStatement ps = conn.prepareStatement(SAVE_SQL)) {
                //"merge into Account (balance, lastoperation, upd_date) key (id) VALUES (?, ?, ?)")) {
                for (Cache.Entry<? extends Long, ? extends Account> entry : entries) {
                    Account val = entry.getValue();
                    ps.setDouble(1, val.getBalance());
                    ps.setDouble(2, val.getLastOperation());
                    ps.setLong(3, val.getUpd_date());
                    ps.setLong(4, val.getId());
                    ps.addBatch();
                }
                ps.executeBatch();
            }
        } catch (SQLException e) {
            throw new CacheWriterException("Failed to writeAll: " + entries, e);
        }
    }

    @Override
    public void delete(Object key) throws CacheWriterException {
        try (Connection conn = connection()) {

            try (PreparedStatement ps = conn.prepareStatement(DELETE_SQL)) {
                ps.setLong(1, (Long) key);
                ps.executeUpdate();
            }
        } catch (SQLException e) {
            throw new CacheWriterException("Failed to delete: " + key, e);
        }
    }

    @Override
    public void deleteAll(Collection<?> keys) throws CacheWriterException {
        try (Connection conn = connection()) {
            try (PreparedStatement st = conn.prepareStatement(DELETE_SQL)) {
                for (Object key : keys) {
                    st.setLong(1, (Long) key);

                    st.addBatch();
                }
                st.executeBatch();
            }
        } catch (SQLException e) {
            throw new CacheWriterException("Failed to deleteAll: " + keys, e);
        }
    }

    @Override
    public void sessionEnd(boolean commit) {
/*        try {
            Connection conn = session.attachment();
            if (conn != null && session.isWithinTransaction()) {
                if (commit)

                    conn.commit();
                else
                    conn.rollback();
            }
        } catch (SQLException e) {
            throw new CacheWriterException("Failed to end store session.", e);
        }*/
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

}
