package com.krootix;

import com.krootix.dao.AccountDao;
import com.krootix.dao.AccountDaoJdbcImpl;
import com.krootix.entity.Account;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;
import org.apache.ignite.transactions.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.cache.configuration.FactoryBuilder;
import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.*;

import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

public class app {
    public static Logger LOGGER = LoggerFactory.getLogger(app.class);
    private static CacheConfiguration cacheCfg;
    private static IgniteConfiguration cfg;
    private static AccountDao accountDao;

    private static void configurationIgnite() {
        /*CacheConfiguration*/
        cacheCfg = new CacheConfiguration<>("cacheAccount");
        cacheCfg.setAtomicityMode(CacheAtomicityMode.ATOMIC); //TRANSACTIONAL);
        cacheCfg.setName("cacheAccount");

//        cacheCfg.setCacheStoreFactory(FactoryBuilder.factoryOf(CacheAccountStore.class));
        cacheCfg.setCacheStoreFactory(FactoryBuilder.factoryOf(CacheJdbcAccountStore.class));

        cacheCfg.setReadThrough(true);
        cacheCfg.setWriteThrough(true);

        QueryEntity qe = new QueryEntity(Long.class, Account.class);
        LinkedHashMap linkedHashMap = new LinkedHashMap();
        linkedHashMap.put("id", "java.lang.Long");
        linkedHashMap.put("name", "java.lang.String");
        linkedHashMap.put("balance", "java.lang.Double");
        linkedHashMap.put("lastOperation", "java.lang.Double");
        linkedHashMap.put("upd_date", "java.lang.Long");
        qe.setFields(linkedHashMap);
        Collection<QueryEntity> collection = new ArrayList<>();
        collection.add(qe);
        cacheCfg.setQueryEntities(collection);

        // create a new instance of TCP Discovery SPI
        TcpDiscoverySpi spi = new TcpDiscoverySpi();
        // create a new instance of tcp discovery multicast ip finder
        TcpDiscoveryMulticastIpFinder tcMp = new TcpDiscoveryMulticastIpFinder();
        tcMp.setAddresses(Arrays.asList("localhost")); // change your IP address here
        // set the multi cast ip finder for spi
        spi.setIpFinder(tcMp);
        // create new ignite configuration
        /*IgniteConfiguration*/
        cfg = new IgniteConfiguration();
        cfg.setCacheConfiguration(cacheCfg);
        //cfg.setIgniteInstanceName("IgniteDataNode");
        cfg.setClientMode(false);
        // set the discovery§ spi to ignite configuration
        cfg.setDiscoverySpi(spi);
        // Optional transaction configuration. Configure TM lookup here.
        TransactionConfiguration txCfg = new TransactionConfiguration();
        cfg.setTransactionConfiguration(txCfg);
        // Start Ignite node.
    }

    public static void main(String[] args) {

        accountDao = new AccountDaoJdbcImpl();

        try {
            ((AccountDaoJdbcImpl) accountDao).createDbAccount();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }

        configurationIgnite();
        Ignite ignite = Ignition.start(cfg);

        IgniteCache<Long, Account> cacheAccountA = ignite.getOrCreateCache(cacheCfg);
        IgniteCache<Long, Account> cacheAccountB = ignite.getOrCreateCache(cacheCfg);

        double balance = 5000;
        double lastOperation = balance;

        Account accountA = new Account(1, "John", balance, lastOperation, new Date().getTime());
        Account accountB = new Account(2, "Nick", balance, lastOperation, new Date().getTime());

        LOGGER.info("Account A: " + accountA);
        LOGGER.info("Account B: " + accountB);

        cacheAccountA.put(accountA.getId(), accountA);
        cacheAccountB.put(accountB.getId(), accountB);

        executeTransaction(1L, 2L, cacheAccountA, cacheAccountB);//, iRandom);

        // output from mysql without ignite
        try {
            long generalBalance = 0;
            List<Account> accounts = accountDao.selectAll();//new ArrayList<Account>();
            accounts.stream().forEach(a -> LOGGER.info(a.toString()));
            double sum = accounts.stream().mapToDouble(o -> o.getBalance()).sum();
            LOGGER.info("Total: " + sum);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        ignite.close();
    }

    private static double getRandomNumberInRange(int min, int max) {
        Random r = new Random();
        double randomDouble = r.doubles(min, (max + 1)).limit(1).findFirst().getAsDouble();
        return aRound(randomDouble);
    }

    private static double aRound(double aRound) {
        return Math.round(aRound * 100.0) / 100.0;
    }

    private static void executeTransaction(long aL, long bL, IgniteCache<Long, Account> cacheA, IgniteCache<Long, Account> cacheB) {//}, long amount) {
        try (Transaction tx = Ignition.ignite().transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {

            int min = -50;
            int max = 50;

            double amount;

            Account accountA;//= cacheA.get(aL);
            Account accountB;//= cacheB.get(bL);
            //Account accountA = cacheA.get(aL);
            //Account accountB = cacheB.get(bL);
            //LOGGER.info("In Ignite transaction: ");
            //LOGGER.info("before");
            //LOGGER.info(accountA.toString());
            //LOGGER.info(accountB.toString());

            for (int i = 0; i < 50; i++) {
                amount = getRandomNumberInRange(min, max);
                LOGGER.info("Amount to transfer: " + amount);
                accountA = cacheA.get(aL);
                accountB = cacheB.get(bL);
                if (amount > accountA.getBalance())
                    throw new IllegalArgumentException(
                            "Transfer cannot be completed");

                accountA.setBalance(aRound(accountA.getBalance() - amount));
                accountA.setLastOperation(-amount);
                accountA.setUpd_date(new Date().getTime());
                accountB.setBalance(aRound(accountB.getBalance() + amount));
                accountB.setLastOperation(amount);
                accountB.setUpd_date(new Date().getTime());
                LOGGER.info("Account to:" + accountB.toString());
                LOGGER.info("Account from:" + accountA.toString());

                cacheA.put(aL, accountA);
                cacheB.put(bL, accountB);
            }

            tx.commit();

            //LOGGER.info("after");
            //LOGGER.info(accountA.toString());
            //LOGGER.info(accountB.toString());

            //cacheA.put(aL, accountA);
            //cacheB.put(bL, accountB);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
