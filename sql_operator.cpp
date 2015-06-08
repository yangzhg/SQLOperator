// Copyright (c) 2015 shadamao. All Rights Reserved.
//  Author: shadamao <780531911@qq.com>

#include "sql_operator.h"

#include "thirdparty/mysql-connector-c++/mysql_driver.h"

namespace sqloperator {

DECLARE_string(op_host_name);
DECLARE_string(op_user);
DECLARE_string(op_pwd);

DEFINE_string(op_host_name, "tcp://localhost:3306/test", "tcp://ip:port/db");
DEFINE_string(op_user, "root", "operator db user");
DEFINE_string(op_pwd, "123456", "operator db pwd");

// the way to get connection is temporary
// this will be replace by connection pool or others

SQLOperator::SQLOperator(
        const std::string& host_name,
        const std::string& user,
        const std::string& pwd) : _in_transaction(false) {
    load_driver();
    _options["hostName"] = host_name;
    _options["userName"] = user;
    _options["password"] = pwd;
    get_connection();
}

SQLOperator::SQLOperator() : _in_transaction(false) {
    load_driver();
    _options["hostName"] = FLAGS_op_host_name;
    _options["userName"] = FLAGS_op_user;
    _options["password"] = FLAGS_op_pwd;
    get_connection();
}

SQLOperator::~SQLOperator() {
    if (_con != NULL && !_con->isClosed()) {
        try {
            _con->close();
        } catch (const sql::SQLException &e) {
            LOG(ERROR) << "connection close error, msg:" << e.what()
                       << " (MySQL error code: " << e.getErrorCode()
                       << ", SQLState: " << e.getSQLState() << " )";
        } catch (...) {
            LOG(ERROR) << "connection can't close";
        }
    }
}

bool SQLOperator::execute(const std::string& sql, int32_t* ret) {
    SQLOPERATOR_GET_CONNECTION;
    try {
        std::unique_ptr<sql::Statement> stmt(_con->createStatement());
        *ret = stmt->executeUpdate(sql);
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

bool SQLOperator::execute_query(const std::string& sql, std::shared_ptr<sql::ResultSet>* result) {
    SQLOPERATOR_GET_CONNECTION;
    try {
        std::unique_ptr<sql::Statement> stmt(_con->createStatement());
        std::shared_ptr<sql::ResultSet> result_set(stmt->executeQuery(sql));
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

bool SQLOperator::execute_batch(
        const std::vector<std::string>& sqls,
        std::vector<int32_t>* effect_rows) {
    if (_in_transaction) {
        return false;
    }
    SQLOPERATOR_GET_CONNECTION;
    try {
        std::unique_ptr<sql::Statement> stmt(_con->createStatement());
        stmt->execute("START TRANSACTION;");
        for (std::vector<std::string>::const_iterator it = sqls.begin(); it != sqls.end(); ++it) {
            effect_rows->push_back(stmt->executeUpdate(*it));
        }
        stmt->execute("COMMIT;");
    } catch (const sql::SQLException& e) {
        VLOG(20) << "SQLException:" << e.what()
                 << " (MySQL error code: " << e.getErrorCode()
                 << ", SQLState: " << e.getSQLState() << " )";
        try {
            std::unique_ptr<sql::Statement> stmt_t(_con->createStatement());
            stmt_t->execute("ROLLBACK;");
        } catch (const sql::SQLException& exc) {
            VLOG(20) << "SQLException:" << exc.what()
                     << " (MySQL error code: " << exc.getErrorCode()
                     << ", SQLState: " << exc.getSQLState() << " )";
            return false;
        }
        return false;
    }
    return true;
}

bool SQLOperator::start_transaction() {
    if (_in_transaction) {
        return false;
    }
    int ret = 0;
    RETURN_IF_ERROR(execute("START TRANSACTION;", &ret));
    _in_transaction = true;
    return true;
}
bool SQLOperator::commit() {
    if (!_in_transaction) {
        return true;
    }
    int ret = 0;
    RETURN_IF_ERROR(execute("COMMIT;", &ret));
    _in_transaction = false;
    return true;
}

bool SQLOperator::roll_back() {
    if (!_in_transaction) {
        return true;
    }
    int ret = 0;
    RETURN_IF_ERROR(execute("ROLLBACK;", &ret));
    _in_transaction = false;
    return true;
}

// TODO  getconnection may use connection pool,just modify this function
void SQLOperator::get_connection() {
    int i = 1;
    while ((_con == NULL || _con->isClosed()) && i <= 3) {
        if (i != 1) {
            VLOG(20) << "waiting for retry to get connection";
            sleep(1);
        }
        _con.reset();
        CHECK_NOTNULL(_s_driver);
        try {
            _con.reset(_s_driver->connect(_options));
        } catch (const sql::SQLException &e) {
            LOG(ERROR) << "connect to mysql error, msg:" << e.what()
                       << " (MySQL error code: " << e.getErrorCode()
                       << ", SQLState: " << e.getSQLState() << " )";
        } catch (...) {
            LOG(ERROR) << "sql operator can't connect to mysql";
        }
        VLOG(20) << "sql operator create connection for " << i << " times!";
        i++;
    }
    if (_con == NULL || _con->isClosed()) {
        LOG(ERROR) << "completely dead, connection close strangely";
    }
}
void SQLOperator::load_driver() {
    if (_s_driver == NULL) {
        try {
            _s_driver = sql::mysql::get_driver_instance();
        } catch (const sql::SQLException &e) {
            LOG(ERROR) << "load driver error, msg:" << e.what()
                       << " (MySQL error code: " << e.getErrorCode()
                       << ", SQLState: " << e.getSQLState() << " )";
        } catch (...) {
            LOG(ERROR) << "sql operator can't load driver";
        }
    }
    CHECK_NOTNULL(_s_driver);
}
} // namespace sqloperator

