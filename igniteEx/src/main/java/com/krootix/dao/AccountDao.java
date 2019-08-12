package com.krootix.dao;

import com.krootix.entity.Account;

import java.sql.SQLException;
import java.util.List;

public interface AccountDao {
    public List<Account> selectAll() throws SQLException;

}
