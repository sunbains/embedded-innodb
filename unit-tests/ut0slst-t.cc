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

namespace logger {
int level = (int)Level::Debug;
const char *Progname = "ut0slst-t";
}  // namespace logger

struct TestNode {
  ut_slist_node<TestNode> m_node;
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
