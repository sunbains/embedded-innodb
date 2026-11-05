/****************************************************************************
Copyright (c) 2026 Sunny Bains. All rights reserved.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free Software
Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc., 59 Temple
Place, Suite 330, Boston, MA 02111-1307 USA

*****************************************************************************/

/** @file unit-tests/test_pars.cc
Parser: the internal SQL, as used by the dictionary. Then each construct of the
grammar on a shared instance (ParsDb): literals, arithmetic, comparisons,
IF/ELSIF/ELSE, WHILE/FOR/EXIT/RETURN, functions, NULL, bound literals and ids,
SELECT INTO with aggregates, NOTFOUND, cursors, bound functions, DML on the SYS_
tables rolled back and committed, DML on a user table, FOR UPDATE locks, CREATE
TABLE/INDEX, predefined procedures, the fatal syntax error; and the performance
of que_eval_sql() (ParsPerf). */

#include "ut0env.h"
#include "ut0perf.h"

#include "data0type.h"
#include "dict0dict.h"
#include "mach0data.h"
#include "pars0pars.h"
#include "que0que.h"
#include "row0sel.h"
#include "trx0trx.h"

#include <format>

namespace {

using ParsTest = ut::DbTest;

/** Run sql in trx, which has the dictionary locked. */
DbErr eval(System &sys, Trx *trx, ParsInfo *info, const char *sql) {
  /* schema_lock_exclusive() holds the dictionary mutex. */
  return que_eval_sql(sys, info, sql, false, trx);
}

/** An empty procedure. */
TEST_F(ParsTest, Null) {
  auto trx = begin();

  ASSERT_EQ(api::schema_lock_exclusive(*m_sys, trx), DB_SUCCESS);
  EXPECT_EQ(eval(*m_sys, trx, pars_info_create(), "PROCEDURE P () IS\nn INT;\nBEGIN\nn := 1;\nEND;\n"), DB_SUCCESS);
  commit(trx);
}

/** A procedure with bound literals that changes the dictionary, the way
TRUNCATE renumbers a table, then reads it back. */
TEST_F(ParsTest, Literals) {
  m_sys->m_dict->mutex_acquire();
  auto table = m_sys->m_dict->table_get(ut::TABLE);
  m_sys->m_dict->mutex_release();

  ASSERT_NE(table, nullptr);

  const auto table_id = table->m_id;
  auto trx = begin();

  ASSERT_EQ(api::schema_lock_exclusive(*m_sys, trx), DB_SUCCESS);

  auto info = pars_info_create();

  pars_info_add_uint64_literal(info, "id", table_id);

  EXPECT_EQ(
    eval(
      *m_sys,
      trx,
      info,
      "PROCEDURE P () IS\n"
      "n INT;\n"
      "BEGIN\n"
      "  n := 0;\n"
      "  SELECT COUNT(*) INTO n FROM SYS_COLUMNS WHERE TABLE_ID = :id;\n"
      "  IF (n = 3) THEN\n"
      "    UPDATE SYS_TABLES SET MIX_LEN = MIX_LEN WHERE ID = :id;\n"
      "  END IF;\n"
      "END;\n"
    ),
    DB_SUCCESS
  );

  commit(trx);
  restart();
  expect_rows({});
}

/** Many DDL statements go through the parser: create and drop indexes. */
TEST_F(ParsTest, Ddl) {
  load({{1, ut::Row{1, 1, "x"}}, {2, ut::Row{2, 2, "y"}}});

  for (int i = 0; i < 5; ++i) {
    auto trx = begin();

    ASSERT_EQ(api::schema_lock_exclusive(*m_sys, trx), DB_SUCCESS);
    ASSERT_EQ(api::table_drop(*m_sys, trx, ut::TABLE), DB_SUCCESS);
    commit(trx);

    stop();
    start(true);
  }

  expect_rows({});
}


/* ------------------------------------------------------------------------
The internal SQL, statement by statement, on one instance shared by the suite
(starting one costs seconds). A procedure hands values back to the test through
a bound function: FETCH c INTO out() calls it with the row of the cursor c, whose
select list can name the procedure's variables. */

/** A value handed back by a procedure: nullopt is SQL NULL. */
using Val = std::optional<std::string>;

/** The rows handed back by a procedure. */
using Rows = std::vector<std::vector<Val>>;

/** The bound function "out": appends the select list values of the cursor row. */
void *out(void *arg, void *user_arg) {
  auto sel = static_cast<SelNode *>(arg);
  auto rows = static_cast<Rows *>(user_arg);
  std::vector<Val> row;

  for (auto exp = sel->m_select_list; exp != nullptr; exp = que_node_get_next(exp)) {
    const auto dfield = que_node_get_val(exp);

    if (dfield_is_null(dfield)) {
      row.emplace_back(std::nullopt);
    } else {
      row.emplace_back(std::string(static_cast<const char *>(dfield_get_data(dfield)), dfield_get_len(dfield)));
    }
  }

  rows->push_back(std::move(row));

  return arg;
}

/** @return the 4 byte integer v as the procedure hands it back. */
Val i4(int32_t v) {
  std::string s(4, '\0');

  mach_write_to_4(reinterpret_cast<byte *>(s.data()), uint32_t(v));

  return s;
}

/** A cursor that hands back the variables v (a comma separated list), once. Not
"NAME = ..": see UniqueConsistentRead. */
std::string emitter(const char *name, const char *vars) {
  return std::format(
    "DECLARE FUNCTION out;\n"
    "DECLARE CURSOR {} IS SELECT {} FROM SYS_TABLES WHERE NAME >= 'SYS_FOREIGN' AND NAME <= 'SYS_FOREIGN';\n",
    name,
    vars
  );
}

/** Hand back the row of the emitter cursor "emit". */
constexpr const char *EMIT = "OPEN emit; FETCH emit INTO out(); CLOSE emit;\n";

/** A started instance shared by the suite. Lock waits time out after a second. */
class ParsDb : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    s_dir = new ut::TestDir();

    auto config = ut::make_config(s_dir->m_path);

    config.m_lock.m_wait_timeout = std::chrono::seconds(1);

    s_sys = new System();

    ASSERT_EQ(s_sys->init(config), DB_SUCCESS);
    ASSERT_EQ(s_sys->startup(), DB_SUCCESS);
    ASSERT_TRUE(api::database_create(*s_sys, "test"));
  }

  static void TearDownTestSuite() {
    if (s_sys != nullptr) {
      EXPECT_EQ(s_sys->shutdown(IB_SHUTDOWN_NORMAL), DB_SUCCESS);
      delete s_sys;
      s_sys = nullptr;
    }

    delete s_dir;
    s_dir = nullptr;
  }

  void SetUp() override { ASSERT_NE(s_sys, nullptr); }

  /** @return a new info with the function "out" bound to m_rows. */
  ParsInfo *info() {
    auto info = pars_info_create();

    pars_info_add_function(info, "out", out, &m_rows);

    return info;
  }

  /** Run sql in trx. The parser needs the dictionary mutex: the exclusive schema
  lock holds it, else que_eval_sql() takes it. */
  DbErr run(Trx *trx, const std::string &sql, ParsInfo *pinfo = nullptr) {
    const bool reserve = !api::schema_lock_is_exclusive(trx);

    return que_eval_sql(m_sys, pinfo != nullptr ? pinfo : info(), sql.c_str(), reserve, trx);
  }

  /** Run sql in a trx of its own, committed (or rolled back on an error). */
  DbErr run(const std::string &sql, ParsInfo *pinfo = nullptr) {
    auto trx = locked();
    const auto err = run(trx, sql, pinfo);

    if (err == DB_SUCCESS) {
      EXPECT_EQ(api::trx_commit(m_sys, trx), DB_SUCCESS);
    } else {
      EXPECT_EQ(api::trx_rollback(m_sys, trx), DB_SUCCESS);
    }

    return err;
  }

  /** @return a new trx holding the exclusive schema lock. */
  Trx *locked() {
    auto trx = api::trx_begin(m_sys, IB_TRX_REPEATABLE_READ);

    EXPECT_EQ(api::schema_lock_exclusive(m_sys, trx), DB_SUCCESS);

    return trx;
  }

  /** CREATE TABLE test/<test><suffix> (k INT UNSIGNED, v INT UNSIGNED, PRIMARY KEY(k))
  through the API, with the rows (k, k * 10) for k in [0, n). @return its id */
  uint64_t create_kv(const std::string &name, uint32_t n) {
    api::TableSchema *sch{};
    api::IndexSchema *idx{};

    EXPECT_EQ(api::TableSchema::create(name.c_str(), &sch, IB_TBL_V1, 0), DB_SUCCESS);
    EXPECT_EQ(sch->add_col("k", IB_INT, IB_COL_UNSIGNED, 0, 4), DB_SUCCESS);
    EXPECT_EQ(sch->add_col("v", IB_INT, IB_COL_UNSIGNED, 0, 4), DB_SUCCESS);
    EXPECT_EQ(sch->add_index("PRIMARY", &idx), DB_SUCCESS);
    EXPECT_EQ(idx->add_col("k", 0), DB_SUCCESS);
    EXPECT_EQ(idx->set_clustered(), DB_SUCCESS);

    auto trx = locked();
    uint64_t id{};

    EXPECT_EQ(api::table_create(m_sys, trx, sch, &id), DB_SUCCESS);
    EXPECT_EQ(api::trx_commit(m_sys, trx), DB_SUCCESS);
    api::TableSchema::destroy(m_sys, sch);

    trx = api::trx_begin(m_sys, IB_TRX_REPEATABLE_READ);

    api::Cursor *crsr{};

    EXPECT_EQ(api::cursor_open_table(m_sys, name.c_str(), trx, &crsr), DB_SUCCESS);
    EXPECT_EQ(crsr->lock(m_sys, IB_LOCK_IX), DB_SUCCESS);

    auto tpl = crsr->clust_read_tuple_create();

    for (uint32_t k = 0; k < n; ++k) {
      EXPECT_EQ(tpl->write_u32(0, k), DB_SUCCESS);
      EXPECT_EQ(tpl->write_u32(1, k * 10), DB_SUCCESS);
      EXPECT_EQ(crsr->insert_row(m_sys, tpl), DB_SUCCESS);
    }

    api::Tuple::destroy(tpl);
    EXPECT_EQ(api::Cursor::close(m_sys, crsr), DB_SUCCESS);
    EXPECT_EQ(api::trx_commit(m_sys, trx), DB_SUCCESS);

    return id;
  }

  /** @return the table name of the test. */
  static std::string table(const std::string &suffix = "") {
    return std::format("test/{}{}", ::testing::UnitTest::GetInstance()->current_test_info()->name(), suffix);
  }

  static inline ut::TestDir *s_dir{};
  static inline System *s_sys{};

  System &m_sys{*s_sys};
  Rows m_rows;

  /** The table name of the test: the parser keeps a pointer to a bound id. */
  const std::string m_name{table()};
};

/** Integer literals and arithmetic: + - * /, unary minus, precedence, parentheses. */
TEST_F(ParsDb, Arithmetic) {
  ASSERT_EQ(
    run(std::string("PROCEDURE P () IS\n"
                    "a INT;\n"
                    "b INT;\n"
                    "c INT;\n"
                    "d INT;\n"
                    "e INT;\n") +
        emitter("emit", "a, b, c, d, e") +
        "BEGIN\n"
        "  a := 7 * 6 - 2;\n"
        "  b := 2 + 3 * 4;\n"
        "  c := (2 + 3) * 4;\n"
        "  d := 17 / 5;\n"
        "  e := -5 - -3;\n" +
        EMIT + "END;\n"),
    DB_SUCCESS
  );

  ASSERT_EQ(m_rows.size(), 1UL);
  EXPECT_EQ(m_rows[0], (std::vector<Val>{i4(40), i4(14), i4(20), i4(3), i4(-2)}));
}

/** Comparisons and logic: = <> < <= > >=, AND OR NOT, on integers and strings. */
/** Signed columns are signed and unsigned ones unsigned, wherever a value crosses between
the evaluator and the table: read, compared, searched, inserted, updated. On a page a signed
integer has its sign bit flipped, so that bytes sort as numbers; in the evaluator it is a
plain two's complement integer. */
TEST_F(ParsDb, SignedColumns) {
  api::TableSchema *sch{};
  api::IndexSchema *idx{};

  ASSERT_EQ(api::TableSchema::create(m_name.c_str(), &sch, IB_TBL_V1, 0), DB_SUCCESS);
  ASSERT_EQ(sch->add_col("k", IB_INT, IB_COL_UNSIGNED, 0, 4), DB_SUCCESS);
  ASSERT_EQ(sch->add_col("s", IB_INT, IB_COL_NONE, 0, 4), DB_SUCCESS);
  ASSERT_EQ(sch->add_col("u", IB_INT, IB_COL_UNSIGNED, 0, 4), DB_SUCCESS);
  ASSERT_EQ(sch->add_index("PRIMARY", &idx), DB_SUCCESS);
  ASSERT_EQ(idx->add_col("k", 0), DB_SUCCESS);
  ASSERT_EQ(idx->set_clustered(), DB_SUCCESS);
  ASSERT_EQ(sch->add_index("S", &idx), DB_SUCCESS);
  ASSERT_EQ(idx->add_col("s", 0), DB_SUCCESS);

  auto trx = locked();
  uint64_t id{};

  ASSERT_EQ(api::table_create(m_sys, trx, sch, &id), DB_SUCCESS);
  ASSERT_EQ(api::trx_commit(m_sys, trx), DB_SUCCESS);
  api::TableSchema::destroy(m_sys, sch);

  trx = api::trx_begin(m_sys, IB_TRX_REPEATABLE_READ);

  api::Cursor *crsr{};

  ASSERT_EQ(api::cursor_open_table(m_sys, m_name.c_str(), trx, &crsr), DB_SUCCESS);
  ASSERT_EQ(crsr->lock(m_sys, IB_LOCK_IX), DB_SUCCESS);

  /* (k, s, u): the signed values around 0, the unsigned one above the int32 range. */
  const std::vector<std::tuple<uint32_t, int32_t, uint32_t>> rows{{0, -5, 1}, {1, 0, 2}, {2, 5, 4000000000U}};

  for (const auto &[k, sv, u] : rows) {
    auto tpl = crsr->clust_read_tuple_create();

    ASSERT_EQ(tpl->write_u32(0, k), DB_SUCCESS);
    ASSERT_EQ(tpl->write_i32(1, sv), DB_SUCCESS);
    ASSERT_EQ(tpl->write_u32(2, u), DB_SUCCESS);
    ASSERT_EQ(crsr->insert_row(m_sys, tpl), DB_SUCCESS);
    api::Tuple::destroy(tpl);
  }

  ASSERT_EQ(api::Cursor::close(m_sys, crsr), DB_SUCCESS);
  ASSERT_EQ(api::trx_commit(m_sys, trx), DB_SUCCESS);

  auto bind = [&] {
    auto pinfo = info();

    pars_info_add_id(pinfo, "t", m_name.c_str());

    return pinfo;
  };

  ASSERT_EQ(
    run(std::string("PROCEDURE P () IS\n"
                    "x INT;\n"
                    "neg INT;\n"
                    "nonneg INT;\n"
                    "total INT;\n"
                    "ranged INT;\n"
                    "ubig INT;\n"
                    "after INT;\n") +
        emitter("emit", "x, neg, nonneg, total, ranged, ubig, after") +
        "BEGIN\n"
        "  SELECT s INTO x FROM $t WHERE k >= 0 AND k <= 0;\n"
        "  SELECT COUNT(*) INTO neg FROM $t WHERE s < 0;\n"
        "  SELECT COUNT(*) INTO nonneg FROM $t WHERE s >= 0;\n"
        "  SELECT SUM(s) INTO total FROM $t;\n"
        "  SELECT COUNT(*) INTO ranged FROM $t WHERE s >= -6 AND s <= 0;\n"
        "  SELECT COUNT(*) INTO ubig FROM $t WHERE u > 3000000000;\n"
        "  INSERT INTO $t VALUES (3, -7, 9);\n"
        "  UPDATE $t SET s = -8 WHERE k = 1;\n"
        "  SELECT COUNT(*) INTO after FROM $t WHERE s < 0;\n" +
        EMIT + "END;\n",
      bind()),
    DB_SUCCESS
  );

  ASSERT_EQ(m_rows.size(), 1UL);
  EXPECT_EQ(m_rows[0][0], i4(-5)) << "a signed column read";
  EXPECT_EQ(m_rows[0][1], i4(1)) << "s < 0";
  EXPECT_EQ(m_rows[0][2], i4(2)) << "s >= 0";
  EXPECT_EQ(m_rows[0][3], i4(0)) << "SUM(s)";
  EXPECT_EQ(m_rows[0][4], i4(2)) << "a range on the signed index";
  EXPECT_EQ(m_rows[0][5], i4(1)) << "u above int32";
  EXPECT_EQ(m_rows[0][6], i4(3)) << "after the insert and the update";

  /* What the SQL wrote, through the API. */
  trx = api::trx_begin(m_sys, IB_TRX_REPEATABLE_READ);
  ASSERT_EQ(api::cursor_open_table(m_sys, m_name.c_str(), trx, &crsr), DB_SUCCESS);

  std::map<uint32_t, int32_t> got;
  auto tpl = crsr->clust_read_tuple_create();

  for (auto err = crsr->first(m_sys); err == DB_SUCCESS; err = crsr->next(m_sys)) {
    uint32_t k{};
    int32_t sv{};

    ASSERT_EQ(crsr->read_row(m_sys, tpl), DB_SUCCESS);
    ASSERT_EQ(tpl->read_u32(0, &k), DB_SUCCESS);
    ASSERT_EQ(tpl->read_i32(1, &sv), DB_SUCCESS);
    got[k] = sv;
    tpl = tpl->clear();
  }

  api::Tuple::destroy(tpl);
  EXPECT_EQ(api::Cursor::close(m_sys, crsr), DB_SUCCESS);
  EXPECT_EQ(api::trx_commit(m_sys, trx), DB_SUCCESS);

  EXPECT_EQ(got, (std::map<uint32_t, int32_t>{{0, -5}, {1, -8}, {2, 5}, {3, -7}}));
}

TEST_F(ParsDb, Comparisons) {
  ASSERT_EQ(
    run(std::string("PROCEDURE P () IS\n"
                    "r INT;\n") +
        emitter("emit", "r") +
        "BEGIN\n"
        "  r := 0;\n"
        "  IF (1 = 1) THEN r := r + 1; END IF;\n"
        "  IF (1 <> 2) THEN r := r + 2; END IF;\n"
        "  IF (1 < 2) AND (2 <= 2) THEN r := r + 4; END IF;\n"
        "  IF (3 > 2) AND (3 >= 3) THEN r := r + 8; END IF;\n"
        "  IF (1 > 2) OR (2 > 1) THEN r := r + 16; END IF;\n"
        "  IF NOT (1 = 2) THEN r := r + 32; END IF;\n"
        "  IF ('abc' < 'abd') THEN r := r + 64; END IF;\n"
        "  IF ('abc' = 'abc') AND ('a' <> 'b') THEN r := r + 128; END IF;\n"
        "  IF (0 < 1) THEN r := r + 256; END IF;\n"
        "  IF (1 = 2) THEN r := r + 1024; END IF;\n" +
        EMIT + "END;\n"),
    DB_SUCCESS
  );

  ASSERT_EQ(m_rows.size(), 1UL);
  EXPECT_EQ(m_rows[0][0], i4(511));
}

/** IF / ELSIF: exactly one branch, chosen on a bound integer. The grammar has
ELSE or a list of ELSIF, not both. */
TEST_F(ParsDb, IfElsifElse) {
  for (int32_t n : {7, 0, 1, 99}) {
    auto pinfo = info();

    pars_info_add_int4_literal(pinfo, "n", n);

    ASSERT_EQ(
      run(std::string("PROCEDURE P () IS\n"
                      "r INT;\n") +
            emitter("emit", "r") +
            "BEGIN\n"
            "  r := 0;\n"
            "  IF (:n = 7) THEN\n"
            "    r := 1;\n"
            "  ELSIF (:n = 0) THEN\n"
            "    r := 2;\n"
            "  ELSIF (:n = 1) THEN\n"
            "    r := 3;\n"
            "  ELSIF (:n > 1) THEN\n"
            "    r := 4;\n"
            "  END IF;\n"
            "  IF (:n = 0) THEN\n"
            "    r := r + 10;\n"
            "  ELSE\n"
            "    r := r + 20;\n"
            "  END IF;\n" +
            EMIT + "END;\n",
          pinfo),
      DB_SUCCESS
    );
  }

  ASSERT_EQ(m_rows.size(), 4UL);
  EXPECT_EQ(m_rows[0][0], i4(21));
  EXPECT_EQ(m_rows[1][0], i4(12));
  EXPECT_EQ(m_rows[2][0], i4(23));
  EXPECT_EQ(m_rows[3][0], i4(24));
}

/** WHILE and FOR loops, EXIT from the innermost loop, RETURN from the procedure. */
TEST_F(ParsDb, Loops) {
  ASSERT_EQ(
    run(std::string("PROCEDURE P () IS\n"
                    "i INT;\n"
                    "sum INT;\n"
                    "f INT;\n"
                    "x INT;\n"
                    "j INT;\n") +
        emitter("emit", "sum, f, x") +
        "BEGIN\n"
        "  sum := 0;\n"
        "  i := 1;\n"
        "  WHILE i <= 100 LOOP\n"
        "    sum := sum + i;\n"
        "    i := i + 1;\n"
        "  END LOOP;\n"
        "  f := 0;\n"
        "  FOR j IN 1 .. 10 LOOP\n"
        "    f := f + j * j;\n"
        "  END LOOP;\n"
        "  x := 0;\n"
        "  WHILE 1 = 1 LOOP\n"
        "    FOR j IN 1 .. 5 LOOP\n"
        "      IF (j = 3) THEN\n"
        "        EXIT;\n"
        "      END IF;\n"
        "      x := x + 1;\n"
        "    END LOOP;\n"
        "    x := x + 100;\n"
        "    EXIT;\n"
        "  END LOOP;\n" +
        EMIT +
        "  RETURN;\n"
        "  sum := 0;\n" +
        EMIT + "END;\n"),
    DB_SUCCESS
  );

  /* Only the first emit: RETURN ends the procedure. */
  ASSERT_EQ(m_rows.size(), 1UL);
  EXPECT_EQ(m_rows[0], (std::vector<Val>{i4(5050), i4(385), i4(102)}));
}

/** The string functions, and TO_CHAR / TO_NUMBER / TO_BINARY / BINARY_TO_NUMBER. */
TEST_F(ParsDb, Functions) {
  ASSERT_EQ(
    run(std::string("PROCEDURE P () IS\n"
                    "s CHAR;\n"
                    "c1 CHAR;\n"
                    "l INT;\n"
                    "sub CHAR;\n"
                    "pos INT;\n"
                    "miss INT;\n"
                    "tc CHAR;\n"
                    "tn INT;\n"
                    "b CHAR;\n"
                    "bn INT;\n"
                    "q CHAR;\n") +
        emitter("emit", "c1, l, sub, pos, miss, tc, tn, b, bn, q") +
        "BEGIN\n"
        "  s := 'hello';\n"
        "  c1 := CONCAT(s, ', ', 'world');\n"
        "  l := LENGTH(c1);\n"
        "  sub := SUBSTR(c1, 7, 5);\n"
        "  pos := INSTR(c1, 'world');\n"
        "  miss := INSTR(c1, 'xyz');\n"
        "  tc := TO_CHAR(1234);\n"
        "  tn := TO_NUMBER('5678');\n"
        "  b := TO_BINARY(258, 2);\n"
        "  bn := BINARY_TO_NUMBER(b);\n"
        "  q := 'it''s';\n" +
        EMIT + "END;\n"),
    DB_SUCCESS
  );

  ASSERT_EQ(m_rows.size(), 1UL);

  const auto &r = m_rows[0];

  ASSERT_EQ(r.size(), 10UL);
  EXPECT_EQ(r[0], "hello, world");
  EXPECT_EQ(r[1], i4(12));
  EXPECT_EQ(r[2], "world");
  EXPECT_EQ(r[3], i4(8));
  EXPECT_EQ(r[4], i4(0));
  EXPECT_EQ(r[5], "1234");
  EXPECT_EQ(r[6], i4(5678));
  EXPECT_EQ(r[7], std::string("\x01\x02", 2));
  EXPECT_EQ(r[8], i4(258));
  EXPECT_EQ(r[9], "it's");
}

/** NULL (from a NULL column: the parser refuses an assignment of NULL, and a
function of NULL other than a comparison is fatal): handed back as NULL, not
equal to a value. */
TEST_F(ParsDb, Null) {
  const auto id = create_kv(table(), 0);
  auto pinfo = info();

  pars_info_add_uint64_literal(pinfo, "id", id);

  ASSERT_EQ(
    run(std::string("PROCEDURE P () IS\n"
                    "s CHAR;\n"
                    "l INT;\n"
                    "t CHAR;\n") +
          emitter("emit", "s, l, t") +
          "BEGIN\n"
          "  s := 'x';\n"
          "  SELECT CLUSTER_NAME INTO s FROM SYS_TABLES WHERE ID = :id;\n"
          "  IF (s = 'x') THEN l := 1; ELSE l := 2; END IF;\n"
          "  t := CONCAT('a', 'b');\n" +
          EMIT + "END;\n",
        pinfo),
    DB_SUCCESS
  );

  ASSERT_EQ(m_rows.size(), 1UL);
  EXPECT_EQ(m_rows[0], (std::vector<Val>{std::nullopt, i4(2), Val("ab")}));
}

/** Negative integers in functions.
Regression: include/eval0eval.h:155 (eval_node_get_int_val) returns
(lint)mach_read_from_4(): no sign extension on a 64 bit lint, -1234 is read as
4294966062. Upstream (7c21eac0 eval0eval.ic:132) cast to (int) first. TO_CHAR()
of a negative number gives "4294966062", a division of a negative is wrong. */
TEST_F(ParsDb, NegativeNumbers) {
  ASSERT_EQ(
    run(std::string("PROCEDURE P () IS\n"
                    "tc CHAR;\n"
                    "d INT;\n") +
        emitter("emit", "tc, d") +
        "BEGIN\n"
        "  tc := TO_CHAR(-1234);\n"
        "  d := -9 / 3;\n" +
        EMIT + "END;\n"),
    DB_SUCCESS
  );

  ASSERT_EQ(m_rows.size(), 1UL);
  EXPECT_EQ(m_rows[0], (std::vector<Val>{Val("-1234"), i4(-3)}));
}

/** Bound literals of each kind: string, 4 byte and 8 byte integers, a binary id. */
TEST_F(ParsDb, BoundLiterals) {
  auto pinfo = info();

  pars_info_add_str_literal(pinfo, "s", "bound string");
  pars_info_add_int4_literal(pinfo, "i", -42);
  pars_info_add_uint64_literal(pinfo, "id", DICT_TABLES_ID);

  ASSERT_EQ(
    run(std::string("PROCEDURE P () IS\n"
                    "s CHAR;\n"
                    "i INT;\n"
                    "name CHAR;\n") +
        emitter("emit", "s, i, name") +
        "BEGIN\n"
        "  s := :s;\n"
        "  i := :i * 2;\n"
        "  SELECT NAME INTO name FROM SYS_TABLES WHERE ID = :id;\n" +
        EMIT + "END;\n",
      pinfo),
    DB_SUCCESS
  );

  /* SYS_TABLES itself is not a row of SYS_TABLES: name stays NULL. */
  ASSERT_EQ(m_rows.size(), 1UL);
  EXPECT_EQ(m_rows[0][0], "bound string");
  EXPECT_EQ(m_rows[0][1], i4(-84));
  EXPECT_EQ(m_rows[0][2], std::nullopt);
}

/** SELECT COUNT(*) and SUM() INTO variables, on the rows of a table's columns. */
TEST_F(ParsDb, SelectAggregates) {
  const auto id = create_kv(table(), 0);
  auto pinfo = info();

  pars_info_add_uint64_literal(pinfo, "id", id);

  ASSERT_EQ(
    run(std::string("PROCEDURE P () IS\n"
                    "n INT;\n"
                    "s INT;\n"
                    "none INT;\n") +
        emitter("emit", "n, s, none") +
        "BEGIN\n"
        "  SELECT COUNT(*) INTO n FROM SYS_COLUMNS WHERE TABLE_ID = :id;\n"
        "  SELECT SUM(POS) INTO s FROM SYS_COLUMNS WHERE TABLE_ID = :id;\n"
        "  SELECT COUNT(*) INTO none FROM SYS_COLUMNS WHERE TABLE_ID = :id AND POS > 100;\n" +
        EMIT + "END;\n",
      pinfo),
    DB_SUCCESS
  );

  /* k and v: positions 0 and 1. */
  ASSERT_EQ(m_rows.size(), 1UL);
  EXPECT_EQ(m_rows[0], (std::vector<Val>{i4(2), i4(1), i4(0)}));
}

/** SELECT INTO that finds no row: SQL % NOTFOUND, the variable keeps its value. */
TEST_F(ParsDb, SelectNotFound) {
  auto pinfo = info();

  pars_info_add_uint64_literal(pinfo, "none", 0xFFFFFFF0);

  ASSERT_EQ(
    run(std::string("PROCEDURE P () IS\n"
                    "id CHAR;\n"
                    "found INT;\n") +
        emitter("emit", "id, found") +
        "BEGIN\n"
        "  id := 'unchanged';\n"
        "  found := 1;\n"
        "  SELECT NAME INTO id FROM SYS_COLUMNS WHERE TABLE_ID = :none;\n"
        "  IF (SQL % NOTFOUND) THEN\n"
        "    found := 0;\n"
        "  END IF;\n" +
        EMIT + "END;\n",
      pinfo),
    DB_SUCCESS
  );

  ASSERT_EQ(m_rows.size(), 1UL);
  EXPECT_EQ(m_rows[0], (std::vector<Val>{Val("unchanged"), i4(0)}));
}

/** An explicit cursor: OPEN, FETCH into variables until NOTFOUND, CLOSE; ORDER BY ... DESC. */
TEST_F(ParsDb, Cursor) {
  const auto id = create_kv(table(), 0);
  auto pinfo = info();

  pars_info_add_uint64_literal(pinfo, "id", id);

  ASSERT_EQ(
    run(std::string("PROCEDURE P () IS\n"
                    "name CHAR;\n"
                    "pos INT;\n"
                    "n INT;\n") +
        "DECLARE CURSOR cols IS SELECT NAME, POS FROM SYS_COLUMNS WHERE TABLE_ID = :id ORDER BY POS DESC;\n" +
        emitter("emit", "name, pos, n") +
        "BEGIN\n"
        "  n := 0;\n"
        "  OPEN cols;\n"
        "  WHILE 1 = 1 LOOP\n"
        "    FETCH cols INTO name, pos;\n"
        "    IF (cols % NOTFOUND) THEN\n"
        "      EXIT;\n"
        "    END IF;\n"
        "    n := n + 1;\n" +
        "    " + EMIT +
        "  END LOOP;\n"
        "  CLOSE cols;\n"
        "END;\n",
      pinfo),
    DB_SUCCESS
  );

  ASSERT_EQ(m_rows.size(), 2UL);
  EXPECT_EQ(m_rows[0], (std::vector<Val>{Val("v"), i4(1), i4(1)}));
  EXPECT_EQ(m_rows[1], (std::vector<Val>{Val("k"), i4(0), i4(2)}));
}

/** FETCH INTO a bound function: the function sees every row. */
TEST_F(ParsDb, FetchIntoFunction) {
  const auto id = create_kv(table(), 0);
  auto pinfo = info();

  pars_info_add_uint64_literal(pinfo, "id", id);

  ASSERT_EQ(
    run("PROCEDURE P () IS\n"
        "DECLARE FUNCTION out;\n"
        "DECLARE CURSOR cols IS SELECT NAME, MTYPE, LEN FROM SYS_COLUMNS WHERE TABLE_ID = :id;\n"
        "BEGIN\n"
        "  OPEN cols;\n"
        "  WHILE 1 = 1 LOOP\n"
        "    FETCH cols INTO out();\n"
        "    IF (cols % NOTFOUND) THEN\n"
        "      EXIT;\n"
        "    END IF;\n"
        "  END LOOP;\n"
        "  CLOSE cols;\n"
        "END;\n",
        pinfo),
    DB_SUCCESS
  );

  ASSERT_EQ(m_rows.size(), 2UL);
  EXPECT_EQ(m_rows[0], (std::vector<Val>{Val("k"), i4(DATA_INT), i4(4)}));
  EXPECT_EQ(m_rows[1], (std::vector<Val>{Val("v"), i4(DATA_INT), i4(4)}));
}

/** UPDATE, DELETE and INSERT on the SYS_ tables in a trx: visible in the trx, gone after the rollback. */
TEST_F(ParsDb, DmlRolledBack) {
  const auto id = create_kv(table(), 0);
  const std::string sql =
    std::string("PROCEDURE P () IS\n"
                "mix INT;\n"
                "ncols INT;\n"
                "nfc INT;\n") +
    emitter("emit", "mix, ncols, nfc") +
    "BEGIN\n"
    "  SELECT MIX_LEN INTO mix FROM SYS_TABLES WHERE ID = :id;\n"
    "  SELECT COUNT(*) INTO ncols FROM SYS_COLUMNS WHERE TABLE_ID = :id;\n"
    "  SELECT COUNT(*) INTO nfc FROM SYS_FOREIGN_COLS WHERE ID = 'ParsTestFk';\n" +
    EMIT + "END;\n";

  auto bind = [&] {
    auto pinfo = info();

    pars_info_add_uint64_literal(pinfo, "id", id);

    return pinfo;
  };

  auto trx = locked();

  ASSERT_EQ(run(trx, sql, bind()), DB_SUCCESS);

  ASSERT_EQ(
    run(trx,
        "PROCEDURE P () IS\n"
        "BEGIN\n"
        "  UPDATE SYS_TABLES SET MIX_LEN = 77 WHERE ID = :id;\n"
        "  DELETE FROM SYS_COLUMNS WHERE TABLE_ID = :id;\n"
        "  INSERT INTO SYS_FOREIGN_COLS VALUES ('ParsTestFk', 0, 'a', 'b');\n"
        "  INSERT INTO SYS_FOREIGN_COLS VALUES ('ParsTestFk', 1, 'c', 'd');\n"
        "END;\n",
        bind()),
    DB_SUCCESS
  );

  ASSERT_EQ(run(trx, sql, bind()), DB_SUCCESS);
  EXPECT_EQ(api::trx_rollback(m_sys, trx), DB_SUCCESS);

  ASSERT_EQ(run(sql, bind()), DB_SUCCESS);

  ASSERT_EQ(m_rows.size(), 3UL);
  EXPECT_EQ(m_rows[1][0], i4(77));
  EXPECT_EQ(m_rows[1][1], i4(0));
  EXPECT_EQ(m_rows[1][2], i4(2));

  /* Before and after the rollback: the same. */
  EXPECT_EQ(m_rows[0], m_rows[2]);
  EXPECT_NE(m_rows[0][0], i4(77));
  EXPECT_EQ(m_rows[0][1], i4(2));
  EXPECT_EQ(m_rows[0][2], i4(0));
}

/** A committed UPDATE on a SYS_ table is seen by the next trx. */
TEST_F(ParsDb, DmlCommitted) {
  const auto id = create_kv(table(), 0);

  for (int32_t v : {55, 0}) {
    auto pinfo = info();

    pars_info_add_uint64_literal(pinfo, "id", id);
    pars_info_add_int4_literal(pinfo, "v", v);

    ASSERT_EQ(run("PROCEDURE P () IS\nBEGIN\n  UPDATE SYS_TABLES SET MIX_LEN = :v WHERE ID = :id;\nEND;\n", pinfo), DB_SUCCESS);

    pinfo = info();
    pars_info_add_uint64_literal(pinfo, "id", id);

    ASSERT_EQ(
      run(std::string("PROCEDURE P () IS\nmix INT;\n") + emitter("emit", "mix") +
            "BEGIN\n  SELECT MIX_LEN INTO mix FROM SYS_TABLES WHERE ID = :id;\n" + EMIT + "END;\n",
          pinfo),
      DB_SUCCESS
    );
  }

  ASSERT_EQ(m_rows.size(), 2UL);
  EXPECT_EQ(m_rows[0][0], i4(55));
  EXPECT_EQ(m_rows[1][0], i4(0));
}

/** A bound id names a table: the SQL works on the rows of a user table. */
TEST_F(ParsDb, BoundIdUserTable) {
  create_kv(table(), 20);

  auto bind = [&] {
    auto pinfo = info();

    pars_info_add_id(pinfo, "t", m_name.c_str());

    return pinfo;
  };

  ASSERT_EQ(
    run(std::string("PROCEDURE P () IS\n"
                    "n INT;\n"
                    "s INT;\n"
                    "vv INT;\n") +
        emitter("emit", "n, s, vv") +
        "BEGIN\n"
        "  UPDATE $t SET v = v + 1 WHERE k = 5;\n"
        "  DELETE FROM $t WHERE k >= 10;\n"
        "  SELECT COUNT(*) INTO n FROM $t;\n"
        "  SELECT SUM(v) INTO s FROM $t;\n"
        "  SELECT v INTO vv FROM $t WHERE k >= 5 AND k <= 5;\n" +
        EMIT + "END;\n",
      bind()),
    DB_SUCCESS
  );

  /* 10 rows left, sum(k * 10) for k < 10 is 450, + 1. */
  ASSERT_EQ(m_rows.size(), 1UL);

  /* The user columns are INT UNSIGNED: 4 bytes, as the procedure's INT. */
  EXPECT_EQ(m_rows[0], (std::vector<Val>{i4(10), i4(451), i4(51)}));

  /* And the API sees the same rows. */
  auto trx = api::trx_begin(m_sys, IB_TRX_REPEATABLE_READ);
  api::Cursor *crsr{};
  int n{};

  ASSERT_EQ(api::cursor_open_table(m_sys, table().c_str(), trx, &crsr), DB_SUCCESS);

  for (auto err = crsr->first(m_sys); err == DB_SUCCESS; err = crsr->next(m_sys)) {
    ++n;
  }

  EXPECT_EQ(n, 10);
  EXPECT_EQ(api::Cursor::close(m_sys, crsr), DB_SUCCESS);
  EXPECT_EQ(api::trx_commit(m_sys, trx), DB_SUCCESS);
}

/** SELECT ... FOR UPDATE locks the rows: another trx updating one waits and times out. */
TEST_F(ParsDb, SelectForUpdate) {
  create_kv(table(), 3);

  auto bind = [&] {
    auto pinfo = info();

    pars_info_add_id(pinfo, "t", m_name.c_str());

    return pinfo;
  };

  auto holder = api::trx_begin(m_sys, IB_TRX_REPEATABLE_READ);

  ASSERT_EQ(
    run(holder,
        "PROCEDURE P () IS\n"
        "vv INT;\n"
        "BEGIN\n"
        "  SELECT v INTO vv FROM $t WHERE k = 1 FOR UPDATE;\n"
        "END;\n",
        bind()),
    DB_SUCCESS
  );

  auto waiter = api::trx_begin(m_sys, IB_TRX_REPEATABLE_READ);

  EXPECT_EQ(
    run(waiter, "PROCEDURE P () IS\nBEGIN\n  UPDATE $t SET v = 0 WHERE k = 1;\nEND;\n", bind()), DB_LOCK_WAIT_TIMEOUT
  );

  EXPECT_EQ(api::trx_rollback(m_sys, waiter), DB_SUCCESS);
  EXPECT_EQ(api::trx_commit(m_sys, holder), DB_SUCCESS);
}

/** CREATE TABLE and CREATE INDEX in the internal SQL: a table in the dictionary. */
TEST_F(ParsDb, CreateTable) {
  auto pinfo = info();

  pars_info_add_id(pinfo, "t", m_name.c_str());

  ASSERT_EQ(
    run("PROCEDURE P () IS\n"
        "BEGIN\n"
        "  CREATE TABLE $t (A INT, B CHAR, C BINARY(8), D BLOB);\n"
        "  CREATE UNIQUE CLUSTERED INDEX PK ON $t (A);\n"
        "  CREATE INDEX BC ON $t (B, C);\n"
        "END;\n",
        pinfo),
    DB_SUCCESS
  );

  m_sys.m_dict->mutex_acquire();

  auto t = m_sys.m_dict->table_get(table().c_str());

  ASSERT_NE(t, nullptr);
  EXPECT_EQ(t->get_n_user_cols(), 4UL);
  EXPECT_EQ(t->get_nth_col(0)->mtype, DATA_INT);
  EXPECT_EQ(t->get_nth_col(1)->mtype, DATA_VARCHAR);
  EXPECT_EQ(t->get_nth_col(2)->mtype, DATA_FIXBINARY);
  EXPECT_EQ(t->get_nth_col(2)->len, 8U);
  EXPECT_EQ(t->get_nth_col(3)->mtype, DATA_BLOB);
  ASSERT_NE(t->get_index_on_name("PK"), nullptr);
  EXPECT_TRUE(t->get_index_on_name("PK")->is_clustered());
  ASSERT_NE(t->get_index_on_name("BC"), nullptr);
  EXPECT_EQ(t->get_index_on_name("BC")->get_n_ordering_defined_by_user(), 2UL);

  m_sys.m_dict->mutex_release();

  /* Rows through the SQL. */
  pinfo = info();
  pars_info_add_id(pinfo, "t", m_name.c_str());

  ASSERT_EQ(
    run(std::string("PROCEDURE P () IS\n"
                    "n INT;\n") +
          emitter("emit", "n") +
          "BEGIN\n"
          "  INSERT INTO $t VALUES (1, 'one', '12345678', 'blob');\n"
          "  INSERT INTO $t VALUES (2, 'two', '87654321', NULL);\n"
          "  SELECT COUNT(*) INTO n FROM $t WHERE B = 'two';\n" +
          EMIT + "END;\n",
        pinfo),
    DB_SUCCESS
  );

  ASSERT_EQ(m_rows.size(), 1UL);
  EXPECT_EQ(m_rows[0][0], i4(1));
}

/** ASSERT of a true condition and PRINTF are no-ops for the result. */
TEST_F(ParsDb, PredefinedProcedures) {
  ASSERT_EQ(
    run(std::string("PROCEDURE P () IS\n"
                    "n INT;\n") +
        emitter("emit", "n") +
        "BEGIN\n"
        "  n := 3;\n"
        "  ASSERT(n = 3);\n"
        "  PRINTF('n is ', n);\n" +
        EMIT + "END;\n"),
    DB_SUCCESS
  );

  ASSERT_EQ(m_rows.size(), 1UL);
  EXPECT_EQ(m_rows[0][0], i4(3));
}

/** A consistent read of one row by the whole clustered key, the case that
takes the search shortcut.
Regression: row/row0sel.cc:705 (Plan::try_search_shortcut) asserts ut_ad(m_must_get_clust),
but its only caller (row0sel.cc:849) calls it only when !m_must_get_clust; upstream
(7c21eac0 row0sel.c:1042) asserts ut_ad(!plan->must_get_clust). Any such SELECT
aborts a debug build. */
TEST_F(ParsDb, UniqueConsistentRead) {
  ASSERT_EQ(
    run(std::string("PROCEDURE P () IS\n"
                    "n INT;\n") +
        emitter("emit", "n") +
        "BEGIN\n"
        "  n := 0;\n"
        "  SELECT COUNT(*) INTO n FROM SYS_TABLES WHERE NAME = 'SYS_FOREIGN';\n" +
        EMIT + "END;\n"),
    DB_SUCCESS
  );

  ASSERT_EQ(m_rows.size(), 1UL);
  EXPECT_EQ(m_rows[0][0], i4(1));
}

/** A syntax error in the internal SQL is fatal. */
TEST_F(ParsDb, SyntaxErrorIsFatal) {
  GTEST_FLAG_SET(death_test_style, "threadsafe");

  EXPECT_DEATH(
    {
      auto trx = locked();
      (void)run(trx, "PROCEDURE P () IS\nn INT;\nBEGIN\n  n := ;\nEND;\n");
    },
    "PARSER ERROR"
  );
}

/* ---- Performance ---- */

class ParsPerf : public ParsDb {};

/** Parse and run a small procedure: assignments and a loop. */
TEST_F(ParsPerf, EvalSmall) {
  auto trx = locked();

  auto r = ut::bench("eval_small", 2000, [&](uint64_t) {
    EXPECT_EQ(
      run(trx,
          "PROCEDURE P () IS\n"
          "i INT;\n"
          "s INT;\n"
          "BEGIN\n"
          "  s := 0;\n"
          "  FOR i IN 1 .. 10 LOOP\n"
          "    s := s + i;\n"
          "  END LOOP;\n"
          "END;\n"),
      DB_SUCCESS
    );
  });

  EXPECT_EQ(api::trx_commit(m_sys, trx), DB_SUCCESS);
  EXPECT_LT(r.ns_per_op(), 10e6);
}

/** A loop of 100000 iterations in one procedure: the cost of evaluation without parsing. */
TEST_F(ParsPerf, EvalLoop) {
  auto trx = locked();
  constexpr uint64_t n = 100000;

  const auto start = std::chrono::steady_clock::now();

  EXPECT_EQ(
    run(trx,
        std::format("PROCEDURE P () IS\n"
                    "i INT;\n"
                    "s INT;\n"
                    "BEGIN\n"
                    "  s := 0;\n"
                    "  FOR i IN 1 .. {} LOOP\n"
                    "    s := s + i;\n"
                    "  END LOOP;\n"
                    "END;\n",
                    n)),
    DB_SUCCESS
  );

  const std::chrono::duration<double> secs = std::chrono::steady_clock::now() - start;

  EXPECT_EQ(api::trx_commit(m_sys, trx), DB_SUCCESS);

  ut::BenchResult r{"eval_loop_iteration", 1, n, secs.count()};

  ut::report(r);
  EXPECT_LT(r.ns_per_op(), 1e6);
}

/** SELECT COUNT(*) on SYS_COLUMNS by table id. */
TEST_F(ParsPerf, SelectCount) {
  const auto id = create_kv(table(), 0);
  auto trx = locked();

  auto r = ut::bench("select_count", 2000, [&](uint64_t) {
    auto pinfo = info();

    pars_info_add_uint64_literal(pinfo, "id", id);

    EXPECT_EQ(
      run(trx,
          "PROCEDURE P () IS\n"
          "n INT;\n"
          "BEGIN\n"
          "  SELECT COUNT(*) INTO n FROM SYS_COLUMNS WHERE TABLE_ID = :id;\n"
          "END;\n",
          pinfo),
      DB_SUCCESS
    );
  });

  EXPECT_EQ(api::trx_commit(m_sys, trx), DB_SUCCESS);
  EXPECT_LT(r.ns_per_op(), 10e6);
}

}  // namespace
