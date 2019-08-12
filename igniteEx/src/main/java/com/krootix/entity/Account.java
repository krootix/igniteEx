package com.krootix.entity;

import org.apache.ignite.cache.query.annotations.QuerySqlField;

import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;

public class Account implements Serializable {
    @QuerySqlField(index = true)
    private long id;
    @QuerySqlField(index = true)
    private String name;
    @QuerySqlField(index = true)
    private double balance;
    @QuerySqlField(index = true)
    private double lastOperation;
    @QuerySqlField(index = true)
    private long upd_date;

    public Account(long id, String name, double balance, double lastOperation, long upd_date) {
        this.id = id;
        this.name = name;
        this.balance = balance;
        this.lastOperation = lastOperation;
        this.upd_date = upd_date;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public double getBalance() {
        return balance;
    }

    public void setBalance(double balance) {
        this.balance = balance;
    }

    public double getLastOperation() {
        return lastOperation;
    }

    public void setLastOperation(double lastOperation) {
        this.lastOperation = lastOperation;
    }

    public long getUpd_date() {
        return upd_date;
    }

    public void setUpd_date(long upd_date) {
        this.upd_date = upd_date;
    }

    @Override
    public String toString() {
        LocalDateTime date = LocalDateTime.ofInstant(Instant.ofEpochMilli(upd_date)
                , ZoneId.systemDefault());
        /*LocalDate date = Instant.ofEpochMilli(upd_date)
                .atZone(ZoneId.systemDefault())
                .toLocalDate();*/
        return "Account{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", balance=" + balance +
                ", lastOperation=" + lastOperation +
                ", upd_date=" + date + //new Date(upd_date * 1000).toString() +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Account account = (Account) o;
        return id == account.id;
    }

    @Override
    public int hashCode() {
        return (int) (id ^ (id >>> 32));
    }
}
