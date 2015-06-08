// Copyright (c) 2015 shadamao. All Rights Reserved.
//  Author: shadamao <780531911@qq.com>

#ifndef SQL_OPERATOR_H
#define SQL_OPERATOR_H

#include <istream>  // NOLINT
#include <string>
#include <vector>

#include "thirdparty/glog/logging.h"
#include "thirdparty/mysql-connector-c++/cppconn/connection.h"
#include "thirdparty/mysql-connector-c++/cppconn/driver.h"
#include "thirdparty/mysql-connector-c++/cppconn/exception.h"
#include "thirdparty/mysql-connector-c++/cppconn/prepared_statement.h"
#include "thirdparty/mysql-connector-c++/cppconn/resultset.h"
#include "thirdparty/mysql-connector-c++/cppconn/sqlstring.h"
#include "thirdparty/mysql-connector-c++/cppconn/statement.h"

namespace sqloperator {

#ifndef SQLOPERATOR_GET_CONNECTION
#define SQLOPERATOR_GET_CONNECTION \
do { \
    if (!_con || _con->isClosed()) { \
        if (_in_transaction) { \
            _in_transaction = false; \
            return false \
        } \
        get_connection(); \
    } \
} while (false)
#endif

#ifndef SQLOPERATOR_ROLL_BACK
#define SQLOPERATOR_ROLL_BACK \
do { \
    try { \
        std::unique_ptr<sql::Statement> stmt_t(_con->createStatement()); \
        stmt_t->execute("ROLLBACK;"); \
    } catch (const sql::SQLException& exc) { \
        VLOG(20) << "SQLException:" << exc.what() \
                 << " (MySQL error code: " << exc.getErrorCode() \
                 << ", SQLState: " << exc.getSQLState() << " )"; \
        return false \
    } \
} while (false)
#endif

class SQLOperator {
public:
    SQLOperator(const std::string& host_name, const std::string& user, const std::string& pwd);
    SQLOperator();
    virtual ~SQLOperator();

    virtual bool execute(const std::string& sql, int32_t* ret);
    virtual bool execute_query(const std::string& sql, std::shared_ptr<sql::ResultSet>* result);

    virtual bool execute_batch(
            const std::vector<std::string>& sqls,
            std::vector<int32_t>* effect_rows);

    virtual bool start_transaction();
    virtual bool commit();
    virtual bool roll_back();

    // use:
    // operator.execute(
    //     "UPDATE user SET age = ? WHERE username = ?",
    //     ret_code,
    //     age,
    //     username);
    template <typename... Args>
    bool execute(const std::string& sql, int32_t* ret, const Args&... args) {
        SQLOPERATOR_GET_CONNECTION;
        if (sizeof...(args) == 0) {
            return execute(sql, ret);
        }
        try {
            std::unique_ptr<sql::PreparedStatement> pstmt(_con->prepareStatement(sql));
            prepare(pstmt.get(), args...);
            *ret = pstmt->executeUpdate();
        } catch (const sql::SQLException& e) {
            VLOG(20) << "SQLException:" << e.what()
                     << " (MySQL error code: " << e.getErrorCode()
                     << ", SQLState: " << e.getSQLState() << " )";
            if (_in_transaction) {
                try {
                    SQLOPERATOR_ROLL_BACK;
                    _in_transaction = false;
                } catch (const sql::SQLException& exc) {
                    VLOG(20) << "SQLException:" << exc.what()
                             << " (MySQL error code: " << exc.getErrorCode()
                             << ", SQLState: " << exc.getSQLState() << " )";
                    return false;
                }
            }
            return false;
        }
        return true;
    }

    template <typename... Args>
    bool execute_query(
            const std::string& sql,
            std::shared_ptr<sql::ResultSet>* result,
            const Args&... args) {
        SQLOPERATOR_GET_CONNECTION;
        if (sizeof...(args) == 0) {
            return execute_query(sql, result);
        }
        try {
            std::unique_ptr<sql::PreparedStatement> pstmt(_con->prepareStatement(sql));
            prepare(pstmt.get(), args...);
            std::shared_ptr<sql::ResultSet> result_set(pstmt->executeQuery());
            *result = result_set;
        } catch (const sql::SQLException& e) {
            VLOG(20) << "SQLException:" << e.what()
                     << " (MySQL error code: " << e.getErrorCode()
                     << ", SQLState: " << e.getSQLState() << " )";
            if (_in_transaction) {
                try {
                    SQLOPERATOR_ROLL_BACK;
                    _in_transaction = false;
                } catch (const sql::SQLException& exc) {
                    VLOG(20) << "SQLException:" << exc.what()
                             << " (MySQL error code: " << exc.getErrorCode()
                             << ", SQLState: " << exc.getSQLState() << " )";
                    return false;
                }
            }
            return false;
        }
        return true;
    }

private:
    void get_connection();
    void load_driver();

    template <size_t N>
    void set_parameter(sql::PreparedStatement* pstmt, int32_t value) {
        pstmt->setInt(N, value);
    }

    template <size_t N>
    void set_parameter(sql::PreparedStatement* pstmt, uint32_t value) {
        pstmt->setUInt(N, value);
    }

    template <size_t N>
    void set_parameter(sql::PreparedStatement* pstmt, int64_t value) {
        pstmt->setInt64(N, value);
    }

    template <size_t N>
    void set_parameter(sql::PreparedStatement* pstmt, uint64_t value) {
        pstmt->setUInt64(N, value);
    }

    template <size_t N>
    void set_parameter(sql::PreparedStatement* pstmt, std::istream* value) {
        pstmt->setBlob(N, value);
    }

    template <size_t N>
    void set_parameter(sql::PreparedStatement* pstmt, bool value) {
        pstmt->setBoolean(N, value);
    }

    template <size_t N>
    void set_parameter(sql::PreparedStatement* pstmt, double value) {
        pstmt->setDouble(N, value);
    }

    template <size_t N>
    void set_parameter(sql::PreparedStatement* pstmt, const std::string& value) {
        pstmt->setString(N, sql::SQLString(value));
    }

    template <size_t N>
    void set_parameter(sql::PreparedStatement* pstmt, char* value) {
        pstmt->setString(N, sql::SQLString(value));
    }

    template <size_t N>
    void prepare(sql::PreparedStatement* pstmt) {
        return;
    }

    template <size_t N = 1, typename T, typename... Args>
    void prepare(sql::PreparedStatement* pstmt, const T& value, const Args&... args) {
        set_parameter<N>(pstmt, value);
        prepare<N + 1>(pstmt, args...);
    }

    bool _in_transaction;
    sql::Driver* _s_driver;
    sql::ConnectOptionsMap _options;
    std::unique_ptr<sql::Connection> _con;
};

} // namespace sqloperator

#endif // SQL_OPERATOR_H

