// Copyright (c) 2015 shadamao, Inc. All Rights Reserved.
// Author: shadamao <780531911@qq.com>

#include <iostream>
#include <vector>

#include "thirdparty/gtest/gtest.h"

#include "sql_operator.h"

namespace sqloperator {

class SQLOperatorTest : public testing::Test {
public:
    SQLOperatorTest() {}
    virtual ~SQLOperatorTest() {}

    void SetUp() {
        FLAGS_v = 100;
        _op.reset(
                new SQLOperator(
                        "tcp://localhost:8306/op_test",
                        "root",
                        "123456"));
        std::string create_table =
            "CREATE TABLE IF NOT EXISTS test("
            "    id int(10) unsigned NOT NULL AUTO_INCREMENT,"
            "    age int(10),"
            "    name varchar(50),"
            " PRIMARY KEY (id)"
            ");";
        int ret = 0;
        _op->execute(create_table, &ret);
        LOG(INFO) << "create table " << ret;
    }
    void TearDown() {
        int ret = 0;
        _op->execute("drop table test", &ret);
    }
    std::unique_ptr<SQLOperator> _op;
};

TEST_F(SQLOperatorTest, execute_test) {
    std::string sql1 = "insert into test (age,name) values (12,'name1')";
    std::string sql2 = "select * from test";
    std::string sql3 = "insert into test (age,name) values (?,?)";
    std::string sql4 = "select age,name from test where age = ?";
    std::vector<std::string> sqls;
    sqls.push_back("insert into test (age,name) values (14,'name3')");
    sqls.push_back("insert into test (age,name) values (15,'name4')");
    int32_t ret = 0;
    _op->execute(sql1, &ret);
    EXPECT_EQ(ret, 1);
    std::shared_ptr<sql::ResultSet> result;
    _op->execute_query(sql2, &result);
    EXPECT_EQ(1, result->rowsCount());
    while (result->next()) {
        LOG(INFO) << "age:" << result->getInt("age") << ", name:" << result->getString("name");
    }
    ret = 0;
    int32_t age = 13;
    std::string name = "name2";
    _op->execute(sql3, &ret, age, name);
    EXPECT_EQ(ret, 1);
    _op->execute_query(sql4, &result, 13);
    EXPECT_EQ(1, result->rowsCount());
    while (result->next()) {
        LOG(INFO) << "age:" << result->getInt("age") << ", name:" << result->getString("name");
    }
    std::vector<int32_t> rets;
    _op->execute_batch(sqls, &rets);
    EXPECT_EQ(rets[0], 1);
    EXPECT_EQ(rets[1], 1);
    _op->execute_query(sql2, &result);
    while (result->next()) {
        LOG(INFO) << "age:" << result->getInt("age") << ", name:" << result->getString("name");
    }
};
} // namespace sqloperator
