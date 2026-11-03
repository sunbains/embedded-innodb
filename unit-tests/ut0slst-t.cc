/****************************************************************************
Copyright (c) 2024 Sunny Bains. All rights reserved.

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

#include <cstdint>

#include "log0log.h"

#include "gtest/gtest.h"

#include "ut0slst.h"

struct TestNode {
  UtSlistNode<TestNode> m_node;
  int value{};
};

UT_SLIST_NODE_GETTER_DEF(TestNode, m_node)

struct SingleListTest : public ::testing::Test {

  void SetUp() override {
    for (size_t i = 0; i < 3; ++i) {
      nodes[i].value = i + 1;
    }
  }

  using List = UT_SLIST_BASE_NODE_T(TestNode, m_node);

  List list{};
  TestNode nodes[3]{};
};

TEST_F(SingleListTest, EmptyList) {
  EXPECT_TRUE(list.empty());
  EXPECT_EQ(list.size(), 0);
  EXPECT_EQ(list.m_head, nullptr);
  EXPECT_EQ(list.m_tail, nullptr);
}

TEST_F(SingleListTest, PushFront) {
  list.push_front(&nodes[0]);
  EXPECT_FALSE(list.empty());
  EXPECT_EQ(list.size(), 1);
  EXPECT_EQ(list.m_head, &nodes[0]);
  EXPECT_EQ(list.m_tail, &nodes[0]);
  EXPECT_EQ(list.m_head->value, 1);

  list.push_front(&nodes[1]);
  EXPECT_EQ(list.size(), 2);
  EXPECT_EQ(list.m_head, &nodes[1]);
  EXPECT_EQ(list.m_tail, &nodes[0]);
  EXPECT_EQ(list.m_head->value, 2);
  EXPECT_EQ(list.m_tail->value, 1);
}

TEST_F(SingleListTest, PushBack) {
  list.push_back(&nodes[0]);
  EXPECT_FALSE(list.empty());
  EXPECT_EQ(list.size(), 1);
  EXPECT_EQ(list.m_head, &nodes[0]);
  EXPECT_EQ(list.m_tail, &nodes[0]);
  EXPECT_EQ(list.m_head->value, 1);

  list.push_back(&nodes[1]);
  EXPECT_EQ(list.size(), 2);
  EXPECT_EQ(list.m_head, &nodes[0]);
  EXPECT_EQ(list.m_tail, &nodes[1]);
  EXPECT_EQ(list.m_head->value, 1);
  EXPECT_EQ(list.m_tail->value, 2);
}

TEST_F(SingleListTest, Clear) {
  list.push_back(&nodes[0]);
  list.push_back(&nodes[1]);
  EXPECT_EQ(list.size(), 2);

  list.clear();
  EXPECT_TRUE(list.empty());
  EXPECT_EQ(list.size(), 0);
  EXPECT_EQ(list.m_head, nullptr);
  EXPECT_EQ(list.m_tail, nullptr);
}

TEST_F(SingleListTest, Front) {
  EXPECT_EQ(list.front(), nullptr);

  list.push_back(&nodes[0]);
  EXPECT_EQ(list.front(), &nodes[0]);
  EXPECT_EQ(list.front()->value, 1);

  list.push_front(&nodes[1]);
  EXPECT_EQ(list.front(), &nodes[1]);
  EXPECT_EQ(list.front()->value, 2);
}

TEST_F(SingleListTest, Back) {
  EXPECT_EQ(list.back(), nullptr);

  list.push_back(&nodes[0]);
  EXPECT_EQ(list.back(), &nodes[0]);
  EXPECT_EQ(list.back()->value, 1);

  list.push_back(&nodes[1]);
  EXPECT_EQ(list.back(), &nodes[1]);
  EXPECT_EQ(list.back()->value, 2);

  list.push_front(&nodes[2]);
  EXPECT_EQ(list.back(), &nodes[1]);
  EXPECT_EQ(list.back()->value, 2);
}

TEST_F(SingleListTest, Iterator) {
  // Push nodes in order 1,2,3
  for (auto &node : nodes) {
    list.push_back(&node);
  }

  int expected = 1;
  for (auto node : list) {
    EXPECT_EQ(node->value, expected++);
  }
  EXPECT_EQ(expected, 4);  // Verify we iterated over all elements
}

#include "ut0perf.h"

#include <vector>

namespace {

/** @return the values of list, in order. */
std::vector<int> values(SingleListTest::List &list) {
  std::vector<int> v;

  for (auto node : list) {
    v.push_back(node->value);
  }

  return v;
}

}  // namespace

/** Iterating an empty list visits nothing. */
TEST_F(SingleListTest, IterateEmpty) {
  EXPECT_EQ(list.begin(), list.end());
  EXPECT_TRUE(values(list).empty());
}

/** One element is both the head and the tail, and its next is null. */
TEST_F(SingleListTest, Single) {
  nodes[0].m_node.m_next = &nodes[2];

  list.push_back(&nodes[0]);

  EXPECT_EQ(list.front(), list.back());
  /* push_back() clears a stale next pointer. */
  EXPECT_EQ(nodes[0].m_node.m_next, nullptr);
  EXPECT_EQ(values(list), std::vector<int>{1});
}

/** push_front() and push_back() mixed keep the order and the tail. */
TEST_F(SingleListTest, Mixed) {
  list.push_back(&nodes[0]);
  list.push_front(&nodes[1]);
  list.push_back(&nodes[2]);

  EXPECT_EQ(values(list), (std::vector<int>{2, 1, 3}));
  EXPECT_EQ(list.back(), &nodes[2]);
  EXPECT_EQ(nodes[2].m_node.m_next, nullptr);
  EXPECT_EQ(list.size(), 3UL);
}

/** push_front() into an empty list sets the tail too: a push_back() after
it links behind it. */
TEST_F(SingleListTest, FrontThenBack) {
  list.push_front(&nodes[0]);
  list.push_back(&nodes[1]);

  EXPECT_EQ(values(list), (std::vector<int>{1, 2}));
}

/** After clear() the nodes can go into the list again. */
TEST_F(SingleListTest, ClearReuse) {
  for (int round = 0; round < 3; ++round) {
    for (auto &node : nodes) {
      list.push_back(&node);
    }

    EXPECT_EQ(values(list), (std::vector<int>{1, 2, 3}));

    list.clear();
    EXPECT_TRUE(list.empty());
    EXPECT_EQ(list.begin(), list.end());
  }
}

/** A long list counts and orders all its elements. */
TEST(SingleListLong, Order) {
  std::vector<TestNode> v(10000);
  SingleListTest::List list;

  for (size_t i = 0; i < v.size(); ++i) {
    v[i].value = int(i);

    if (i % 2 == 0) {
      list.push_back(&v[i]);
    } else {
      list.push_front(&v[i]);
    }
  }

  EXPECT_EQ(list.size(), v.size());

  /* Odd values descending, then even values ascending. */
  std::vector<int> expected;

  for (int i = int(v.size()) - 1; i >= 0; --i) {
    if (i % 2 == 1) {
      expected.push_back(i);
    }
  }

  for (int i = 0; i < int(v.size()); i += 2) {
    expected.push_back(i);
  }

  std::vector<int> got;

  for (auto node : list) {
    got.push_back(node->value);
  }

  EXPECT_EQ(got, expected);
  EXPECT_EQ(list.back()->value, int(v.size()) - 2);
}

/** The const accessors. */
TEST_F(SingleListTest, Const) {
  list.push_back(&nodes[0]);
  list.push_back(&nodes[1]);

  const auto &c = list;

  EXPECT_EQ(c.front()->value, 1);
  EXPECT_EQ(c.back()->value, 2);
  EXPECT_EQ(c.size(), 2UL);
  EXPECT_FALSE(c.empty());
}

namespace {

/** push_back and a full clear: the cost of building the dyn array block list. */
TEST(SingleListPerf, PushBack) {
  std::vector<TestNode> v(1024);
  SingleListTest::List list;

  auto r = ut::bench("push_back", 20000000, [&](uint64_t i) {
    list.push_back(&v[i & 1023]);

    if ((i & 1023) == 1023) {
      list.clear();
    }
  });

  EXPECT_LT(r.ns_per_op(), 500.0);
}

}  // namespace
