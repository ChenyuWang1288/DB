#include <string>
#include "glog/logging.h"
#include "index/b_plus_tree.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/index_roots_page.h"

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(index_id_t index_id, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_id_(index_id),
      /*root_page_id_ initialized as invalid page id*/
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size){/*rebuild the b+ tree*/
  IndexRootsPage *index_roots_page =
      reinterpret_cast<IndexRootsPage *>(buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID)->GetData());
  page_id_t root_id = INVALID_PAGE_ID;

  if (index_roots_page->GetRootId(index_id, &root_id)) {
    root_page_id_ = root_id;
  }
  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
}


INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Destroy() {
  if (IsEmpty()) {
    return;
  } else {
    BPlusTreePage *root = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root_page_id_)->GetData());
    DestroyPage(root);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::DestroyPage(BPlusTreePage *page) {
  if (page->IsLeafPage()) {
    buffer_pool_manager_->UnpinPage(page->GetPageId(),true);
    buffer_pool_manager_->DeletePage(page->GetPageId());
  } else {
    InternalPage *internal_page = reinterpret_cast<InternalPage *>(page);
    for (int i = 0; i < internal_page->GetSize(); i++) {
      /*get the child and recursively call DestroyPage*/
      BPlusTreePage *child =
          reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(internal_page->ValueAt(i))->GetData());
      DestroyPage(child);
    }
  }
}


/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const { 
  return root_page_id_ == INVALID_PAGE_ID; 
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> &result, int& position, page_id_t &leaf_page_id,Transaction *transaction) {

  if (IsEmpty()) {
    return false;
  } 
  ValueType ret_value;
  LeafPage *target_leaf = reinterpret_cast<LeafPage *>(FindLeafPage(key)->GetData());
  position = target_leaf->KeyIndex(key, comparator_);
  // leaf_page_id = target_leaf->GetPageId();
  if (target_leaf->Lookup(key, ret_value, comparator_)) {
    result.push_back(ret_value);
    buffer_pool_manager_->UnpinPage(target_leaf->GetPageId(), true);
    return true;
  } else {
    buffer_pool_manager_->UnpinPage(target_leaf->GetPageId(), true);
    return false;
  }
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) { 
  /*the code in textbook P643 */ 
  if (IsEmpty()) {
    StartNewTree(key, value);
    return true;
  } else {
    return InsertIntoLeaf(key, value, transaction);
  }
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  Page *page = buffer_pool_manager_->NewPage(root_page_id_);
  UpdateRootPageId(0);//insert a new index
  if (page == nullptr) {
    LOG(WARNING) << "Fail to new page in insertion" << std::endl;
    throw "out of memory";
  }
  BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *insert_pos =
      reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>*>(page->GetData());
  insert_pos->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
  insert_pos->Insert(key, value, comparator_);
  buffer_pool_manager_->UnpinPage(insert_pos->GetPageId(),true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immediately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) { 
  /*caller should ensure that b+ tree is not empty*/
  if (IsEmpty()) {
    LOG(WARNING) << "Try to insert into an empty tree" << std::endl;
    return false;
  }
  /*find the right leaf page to insert*/
  LeafPage *target_leaf = reinterpret_cast<LeafPage *>(FindLeafPage(key)->GetData());
  if (target_leaf->GetSize() < target_leaf->GetMaxSize()) {
    /*if the size can hold one more new value, just insert*/
    int size=target_leaf->Insert(key, value, comparator_);
    buffer_pool_manager_->UnpinPage(target_leaf->GetPageId(),true);
    return (size==-1)?false:true;
  } else {
    int size=target_leaf->Insert(key, value, comparator_);
    if (size == -1) return false;
    LeafPage *copy_leaf = Split(target_leaf);
    InsertIntoParent(target_leaf, copy_leaf->KeyAt(0), copy_leaf,nullptr);
    buffer_pool_manager_->UnpinPage(target_leaf->GetPageId(),true);
    buffer_pool_manager_->UnpinPage(copy_leaf->GetPageId(), true);
    return true;
  }

}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template<typename N>
N *BPLUSTREE_TYPE::Split(N *node) {
  page_id_t created_page_id = INVALID_PAGE_ID;
  N *created_page = reinterpret_cast<N *>(buffer_pool_manager_->NewPage(created_page_id)->GetData());
  /*Init function has its default parameter*/
  if (node->IsLeafPage()) {
    created_page->Init(created_page_id, INVALID_PAGE_ID, leaf_max_size_);
    LeafPage *leaf = reinterpret_cast<LeafPage *> (node);
    leaf->MoveHalfTo(reinterpret_cast<LeafPage*>(created_page));
  }
    
  else {
    created_page->Init(created_page_id, INVALID_PAGE_ID, internal_max_size_);
    InternalPage *internal = reinterpret_cast<InternalPage *> (node);
    internal->MoveHalfTo(reinterpret_cast<InternalPage *> (created_page), buffer_pool_manager_);
  }
  return created_page;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
  /*code in textbook P644*/
  if (old_node->IsRootPage()) {
    /*replace PopulateRootPage()*/
    page_id_t new_root_page_id = INVALID_PAGE_ID;
    InternalPage *new_root =
        reinterpret_cast<InternalPage *>(buffer_pool_manager_->NewPage(new_root_page_id)->GetData());

    if (new_root == nullptr) {
      LOG(WARNING) << "New Page fails " << std::endl;
      return;
    }
    new_root->Init(new_root_page_id, INVALID_PAGE_ID, internal_max_size_);
    root_page_id_ = new_root_page_id;
    UpdateRootPageId(1);
    /*here I choose to maintain the array_[0].first*/
    if (old_node->IsLeafPage()) {
      LeafPage *old_leaf = reinterpret_cast<LeafPage *> (old_node);
      new_root->SetKeyAt(0, old_leaf->KeyAt(0));
    } else {
      InternalPage *old_internal = reinterpret_cast<InternalPage *> (old_node);
      new_root->SetKeyAt(0, old_internal->KeyAt(0));
    }
    /*it's the function of poculate function*/
    /*set key, value, and size of new root*/
    new_root->SetKeyAt(1, key);
    new_root->SetSize(2);
    new_root->SetValueAt(0, old_node->GetPageId());
    new_root->SetValueAt(1, new_node->GetPageId());
    /*make new root as parent as old_node and new_node*/
    old_node->SetParentPageId(new_root_page_id);
    new_node->SetParentPageId(new_root_page_id);
    buffer_pool_manager_->UnpinPage(new_root_page_id,true);
    return;
  }
  page_id_t parent_id = old_node->GetParentPageId();
  InternalPage *parent_node = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parent_id)->GetData());
  /*if parent could hold one more node*/
  if (parent_node->GetSize() < parent_node->GetMaxSize()) {
    parent_node->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
    new_node->SetParentPageId(parent_node->GetPageId());
  } else {
    parent_node->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
    new_node->SetParentPageId(parent_node->GetPageId());
    InternalPage *copy_internal = Split(parent_node);
    InsertIntoParent(parent_node, copy_internal->KeyAt(0), copy_internal);
    buffer_pool_manager_->UnpinPage(copy_internal->GetPageId(),true);
  }

  buffer_pool_manager_->UnpinPage(parent_id,true);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  if (IsEmpty()) return;
  /*find where the key is*/
  LeafPage *target_leaf = reinterpret_cast<LeafPage *>(FindLeafPage(key)->GetData());
  target_leaf->RemoveAndDeleteRecord(key, comparator_);
  /*update the value in ancestor*/
  InternalPage *parent = nullptr;
  int update_index = -1; /*update_index is the updated key index, 
                         if it is 0, we need to go to parent to update*/
  if (!target_leaf->IsRootPage()) {
    parent =
        reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(target_leaf->GetParentPageId())->GetData());
      /*if the key in parent is smaller, 
      the smallest key in target leaf is deleted,we need to update*/
    update_index = parent->ValueIndex(target_leaf->GetPageId());
    parent->SetKeyAt(update_index, target_leaf->KeyAt(0));
  }
  InternalPage *p_parent = nullptr;
  while (update_index == 0 && (!parent->IsRootPage())) {
    p_parent = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parent->GetParentPageId())->GetData());
    update_index = p_parent->ValueIndex(parent->GetPageId());
    p_parent->SetKeyAt(update_index, parent->KeyAt(0));
    /*move higher and unpin*/
    buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
    parent = p_parent;
  }
  if (parent != nullptr) {
    buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
  }


  /*check if this leaf is root and if the size is less than minsize*/
  if (target_leaf->GetSize() < target_leaf->GetMinSize()) {
    CoalesceOrRedistribute(target_leaf, transaction);
  }
  /*else, delete is successful*/
  buffer_pool_manager_->UnpinPage(target_leaf->GetPageId(), true);
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template<typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  /*first process the special case when node is the root*/
  if (node->GetPageId() == root_page_id_) {
    if (node->GetSize() > 1) {
      return false;
    } else {
      AdjustRoot(node);
      return false;
    }
  }
  bool deleted = false; /*denote node is deleted or not*/
  InternalPage *parent =
      reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(node->GetParentPageId())->GetData());
  int this_index = parent->ValueIndex(node->GetPageId());
  N *pre_sibling = nullptr;
  N *next_sibling = nullptr;
  /*if N index is not 0 or the last child, it has previous and next siblings*/
  if (this_index != 0 && this_index != parent->GetSize()-1) {
    pre_sibling = reinterpret_cast<N *>(buffer_pool_manager_->FetchPage(parent->ValueAt(this_index - 1))->GetData());
    next_sibling = reinterpret_cast<N *>(buffer_pool_manager_->FetchPage(parent->ValueAt(this_index + 1))->GetData());
  }
  else if (this_index == 0) {
    next_sibling = reinterpret_cast<N *>(buffer_pool_manager_->FetchPage(parent->ValueAt(this_index + 1))->GetData());
  } else if (this_index == parent->GetSize() - 1) {
    pre_sibling = reinterpret_cast<N *>(buffer_pool_manager_->FetchPage(parent->ValueAt(this_index - 1))->GetData());
  }
  /*first we try distribute*/
  if (pre_sibling&&pre_sibling->GetSize() + node->GetSize() > node->GetMaxSize()) {
    Redistribute(pre_sibling, node, 1);
  } else if (next_sibling&&next_sibling->GetSize() + node->GetSize() > node->GetMaxSize()) {
    Redistribute(next_sibling, node, 0);
  } /*else we try merge*/ 
  else if (pre_sibling&&pre_sibling->GetSize() +node->GetSize() >= pre_sibling->GetMinSize()) {
    deleted = true;
    if (Coalesce(&pre_sibling, &node, &parent, this_index, transaction)) {
      /*means the parent size is too small, process parent*/
      if (!CoalesceOrRedistribute(parent, transaction)) {
        /*parent is not deleted, we will unpin it*/
        buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
      }
    }
  } else if (next_sibling && next_sibling->GetSize() + node->GetSize() >= next_sibling->GetMinSize()) {
    if (Coalesce(&node, &next_sibling, &parent, this_index + 1, transaction)) {
      if (!CoalesceOrRedistribute(parent, transaction)) {
        buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
      }
    }
  }
  if (pre_sibling) {
    buffer_pool_manager_->UnpinPage(pre_sibling->GetPageId(),true);
  }
  if (next_sibling) {
    buffer_pool_manager_->UnpinPage(next_sibling->GetPageId(),true);
  }
  return deleted;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion happened
 */
INDEX_TEMPLATE_ARGUMENTS
template<typename N>
bool BPLUSTREE_TYPE::Coalesce(N **neighbor_node, N **node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent, int index,
                              Transaction *transaction) {
  /* garantee neigbor_node is previous one*/
  /*Move all the key & value pairs from one page to its sibling page */
  if (reinterpret_cast<BPlusTreePage *>(*node)->IsLeafPage())
    reinterpret_cast<LeafPage *>(*node)->MoveAllTo(reinterpret_cast<LeafPage *>(*neighbor_node));
  else {
    int index = reinterpret_cast<InternalPage *>(*node)->ValueIndex((*node)->GetPageId());
    KeyType key = reinterpret_cast<InternalPage *>(*node)->KeyAt(index);
    reinterpret_cast<InternalPage *>(*node)->MoveAllTo(reinterpret_cast<InternalPage *>(*neighbor_node), key,
                                                       buffer_pool_manager_);
  }
  /*delete node*/
  buffer_pool_manager_->UnpinPage((*node)->GetPageId(), true);
  buffer_pool_manager_->DeletePage((*node)->GetPageId());
  *node = nullptr;
  /*process their parent*/
  (*parent)->Remove(index);
  /*if index==1, we need to update the parent's parent to maintain*/
  if (!(*parent)->IsRootPage()&&index==0) {
    InternalPage *p_parent =
        reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage((*parent)->GetParentPageId())->GetData());
    p_parent->SetKeyAt(p_parent->ValueIndex((*parent)->GetPageId()), (*parent)->KeyAt(0));
    buffer_pool_manager_->UnpinPage(p_parent->GetPageId(),true);
  }
  /*check parent*/
  if ((*parent)->GetSize() < (*parent)->GetMinSize())
    return true;
  else
    return false;

}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template<typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
  if (node->IsLeafPage()) {
    if (index == 0) {
      reinterpret_cast<LeafPage *>(neighbor_node)->MoveFirstToEndOf(reinterpret_cast<LeafPage *>(node));
      /*because we move the first element, so we need to update the key in parent*/
      if (!neighbor_node->IsRootPage()) {
        InternalPage *parent = reinterpret_cast<InternalPage *>(
            buffer_pool_manager_->FetchPage(neighbor_node->GetParentPageId())->GetData());
        parent->SetKeyAt(parent->ValueIndex(neighbor_node->GetPageId()), neighbor_node->KeyAt(0));
        buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
      }
    } else {
      reinterpret_cast<LeafPage *>(neighbor_node)->MoveLastToFrontOf(reinterpret_cast<LeafPage *>(node));
      if (!node->IsRootPage()) {
        InternalPage *parent = reinterpret_cast<InternalPage *>(
            buffer_pool_manager_->FetchPage(node->GetParentPageId())->GetData());
        parent->SetKeyAt(parent->ValueIndex(node->GetPageId()), node->KeyAt(0));
        buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
      }
    }
  } else {
    
    KeyType key = reinterpret_cast<InternalPage *>(node)->KeyAt(0);
    if (index == 0) {
      reinterpret_cast<InternalPage *>(neighbor_node)
          ->MoveFirstToEndOf(reinterpret_cast<InternalPage *>(node), key, buffer_pool_manager_);
      /*the same as above to update parent*/
      if (!neighbor_node->IsRootPage()) {
        InternalPage *parent = reinterpret_cast<InternalPage *>(
            buffer_pool_manager_->FetchPage(neighbor_node->GetParentPageId())->GetData());
        parent->SetKeyAt(parent->ValueIndex(neighbor_node->GetPageId()), neighbor_node->KeyAt(0));
        buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
      }
    } else {
      reinterpret_cast<InternalPage *>(neighbor_node)
          ->MoveLastToFrontOf(reinterpret_cast<InternalPage *>(node), key, buffer_pool_manager_);
      if (!node->IsRootPage()) {
        InternalPage *parent =
            reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(node->GetParentPageId())->GetData());
        parent->SetKeyAt(parent->ValueIndex(node->GetPageId()), node->KeyAt(0));
        buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
      }
    }
  }
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happened
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
  if (old_root_node->IsLeafPage()) {
    if (old_root_node->GetSize() == 0) {
      /*case 2*/
      buffer_pool_manager_->UnpinPage(old_root_node->GetPageId(), true);
      buffer_pool_manager_->DeletePage(old_root_node->GetPageId());
      root_page_id_ = INVALID_PAGE_ID;
      UpdateRootPageId(2);
      return true;
    } else
      return false;
  } else if (old_root_node->GetSize() == 1) {
    /*case1*/
    page_id_t child_page = reinterpret_cast<InternalPage *>(old_root_node)->ValueAt(0);
    buffer_pool_manager_->UnpinPage(old_root_node->GetPageId(), true);
    buffer_pool_manager_->DeletePage(old_root_node->GetPageId());
    root_page_id_ = child_page;
    UpdateRootPageId(1);
    BPlusTreePage* new_root=reinterpret_cast<BPlusTreePage*>(buffer_pool_manager_->FetchPage(child_page));
    new_root->SetParentPageId(INVALID_PAGE_ID);
    buffer_pool_manager_->UnpinPage(child_page,true);
    return true;
  } else
    return false;

}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the left most leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin() { 
  KeyType key{};
  LeafPage *target_leaf = reinterpret_cast<LeafPage *> (FindLeafPage(key, true)->GetData());
  return INDEXITERATOR_TYPE(target_leaf, 0, buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  LeafPage *target_leaf = reinterpret_cast<LeafPage *> (FindLeafPage(key, false)->GetData());
  int index = target_leaf->KeyIndex(key, comparator_);
  if (comparator_( target_leaf->KeyAt(index) , key)!=0) {
    return this->End();
  } 
  return INDEXITERATOR_TYPE(target_leaf, index, buffer_pool_manager_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::End() {
 return INDEXITERATOR_TYPE(nullptr, 0, buffer_pool_manager_); 
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * Note: the leaf page is pinned, you need to unpin it after use.
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost) {
    page_id_t target = root_page_id_;
    Page *page = buffer_pool_manager_->FetchPage(root_page_id_);
    BPlusTreePage *bptp = reinterpret_cast<BPlusTreePage *>(page->GetData());
    while (!bptp->IsLeafPage()) {
      InternalPage *internal_page = reinterpret_cast<InternalPage *>(bptp);
      target = leftMost ? internal_page->ValueAt(0) : internal_page->Lookup(key, comparator_);
      buffer_pool_manager_->UnpinPage(bptp->GetPageId(), true);
      page = buffer_pool_manager_->FetchPage(target);
      bptp = reinterpret_cast<BPlusTreePage *>(page->GetData());
    }

    return page;
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
    IndexRootsPage *index_roots_page =
      reinterpret_cast<IndexRootsPage *>(buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID)->GetData());
  //bool flag = false;
  switch(insert_record){
    case 1://update
      index_roots_page->Update(index_id_, root_page_id_);
      break;
    case 2://delete
      index_roots_page->Delete(index_id_);
      break;
    case 0://insert
      index_roots_page->Insert(index_id_, root_page_id_);
      break;
  }
  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
  return;
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId()
        << ",Parent=" << leaf->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId()
          << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId()
        << ",Parent=" << inner->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> "
          << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId()
              << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
      bpm->UnpinPage(internal->ValueAt(i), false);
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Check() {
  bool all_unpinned = buffer_pool_manager_->CheckAllUnpinned();
  if (!all_unpinned) {
    LOG(ERROR) << "problem in page unpin" << endl;
  }
  return all_unpinned;
}

template
class BPlusTree<int, int, BasicComparator<int>>;

template
class BPlusTree<GenericKey<4>, RowId, GenericComparator<4>>;

template
class BPlusTree<GenericKey<8>, RowId, GenericComparator<8>>;

template
class BPlusTree<GenericKey<16>, RowId, GenericComparator<16>>;

template
class BPlusTree<GenericKey<32>, RowId, GenericComparator<32>>;

template
class BPlusTree<GenericKey<64>, RowId, GenericComparator<64>>;
