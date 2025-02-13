#include "common/instance.h"
#include "gtest/gtest.h"
#include "index/b_plus_tree.h"
#include "index/basic_comparator.h"

static const std::string db_name = "bp_tree_insert_test.db";

TEST(BPlusTreeTests, IndexIteratorTest) {
  // Init engine
  DBStorageEngine engine(db_name);
  BasicComparator<int> comparator;
  BPlusTree<int, int, BasicComparator<int>> tree(0, engine.bpm_, comparator, 4, 4);
  // Insert and delete record
  for (int i = 1; i <= 50; i++) {
    tree.Insert(i, i * 100, nullptr);
  }
  for (int i = 2; i <= 50; i += 2) {
    tree.Remove(i);
  }
  // Search keys

  /*modified function interface:
  2 parameter: position, leaf_page_id
  */
  int position = 0;
  page_id_t leaf_page_id = INVALID_PAGE_ID;

  vector<int> v;
  for (int i = 2; i <= 50; i += 2) {
    ASSERT_FALSE(tree.GetValue(i, v, position, leaf_page_id));
  }
  for (int i = 1; i <= 49; i += 2) {
    ASSERT_TRUE(tree.GetValue(i, v, position, leaf_page_id));
    ASSERT_EQ(i * 100, v[v.size() - 1]);
  }
  // Iterator
  int ans = 1;
  for (auto iter = tree.Begin(); iter != tree.End(); ++iter, ans += 2) {
    EXPECT_EQ(ans, (*iter).first);
    EXPECT_EQ(ans * 100, (*iter).second);
  }
}
