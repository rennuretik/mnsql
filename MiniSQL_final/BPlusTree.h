#ifndef __Minisql__BPlusTree__
#define __Minisql__BPlusTree__
#include <vector>
#include <stdio.h>
#include <string.h>
#include "BufferManager.h"
#include "Minisql.h"
#include <string>
using namespace std;

static BufferManager bm;

// node
typedef int offset_num;     // the value of the tree node

template <typename type>
class node{
public:
    size_t count;   // count number of keys
    node* parent;
    vector <type> keys;
    vector <node*> childs;
    vector <offset_num> vals;
    
    node* next_leaf_node;   // point to the next node
    
    bool isLeaf;        // the flag whether this node is leaf
    
private:
    int degree;
    
public:
    //create a new node
    node(int degree,bool newLeaf=false);
    ~node();
    
public:
    bool is_root();
    bool search(type key,size_t &index);    //search a key and return
    node* splite(type &key);
    size_t add(type &key);                  //add the key
    size_t add(type &key,offset_num val);   // add a key-value in the leaf node and return pos
    bool remove_at(size_t index);

};


//BPlusTree
template <typename type>
class BPlusTree
{
private:
    typedef node<type>* Node;

    struct searchNodeParse
    {
        Node pNode;
        size_t index;   // the position
        bool found_f;   // the flag
    };
private:
    string file_name;
    Node root;
    Node leafHead;      // the head of the leaf node
    size_t keyCount;
    size_t level;
    size_t nodeCount;
    fileNode* file;     // the filenode of this tree
    int keySize;        // the size of key
    int degree;
    
public:
    BPlusTree(string m_name,int keySize,int degree);
    ~BPlusTree();

    offset_num search(type& key);   // search the value of specific key
    bool insert_key(type &key,offset_num val);
    bool delete_key(type &key);
    
    void drop_tree(Node node);
    
    void read_all_from_disk();
    void write_back_all_to_disk();
    void read_from_disk(blockNode* btmp);

private:
    void init_tree();       // init
    bool adjustAfterinsert(Node pNode);
    bool adjustAfterDelete(Node pNode);
    void findToLeaf(Node pNode,type key,searchNodeParse &snp);
};

template <class type>
node<type>::node(int m_degree,bool newLeaf):count(0),parent(NULL),next_leaf_node(NULL),isLeaf(newLeaf),degree(m_degree)
{
    for(size_t i = 0;i < degree+1;i ++)
    {
        childs.push_back(NULL);
        keys.push_back(type());
        vals.push_back(offset_num());
    }
    childs.push_back(NULL);
}


template <class type>
node<type>::~node()
{
    
}


template <class type>
bool node<type>::is_root()
{
    if(parent != NULL) return false;
    else return true;
}

//search key
template <class type>
bool node<type>::search(type key,size_t &index)
{
    if(count == 0 )     // no values
    {
        index = 0;
        return false;
    }
    else
    {
        if(keys[count-1] < key)
        {
            index = count;
            return false;
        }
        else if(keys[0] > key)
        {
            index = 0;
            return false;
        }
        else if(count <= 20)
        {
            for(size_t i = 0;i < count;i ++)
            {
                if(keys[i] == key)
                {
                    index = i;
                    return true;
                }
                else if(keys[i] < key)
                {
                    continue;
                }
                else if(keys[i] > key)
                {
                    index = i;
                    return false;
                }
            }
        }
        else if(count > 20)
        {
            size_t left = 0, right = count - 1, pos = 0;
            while(right>left+1)
            {
                pos = (right + left) / 2;
                if(keys[pos] == key)
                {
                    index = pos;
                    return true;
                }
                else if(keys[pos] < key)
                {
                    left = pos;
                }
                else if(keys[pos] > key)
                {
                    right = pos;
                }
            }
            
            // right == left + 1
            if(keys[left] >= key)
            {
                index = left;
                return (keys[left] == key);
            }
            else if(keys[right] >= key)
            {
                index = right;
                return (keys[right] == key);
            }
            else if(keys[right] < key)
            {
                index = right ++;
                return false;
            }
        } // binary search
    }
    return false;
}

template <class type>
node<type>* node<type>::splite(type &key)
{
    size_t minmumNode = (degree - 1) / 2;
    node* newNode = new node(degree,this->isLeaf);
    if(newNode == NULL)
    {
        cout << "Problems in allocate momeory of node in splite node of " << key << endl;
        exit(2);
    }
    
    if(isLeaf)  // leaf node
    {
        key = keys[minmumNode + 1];
        for(size_t i = minmumNode + 1;i < degree;i ++)  // copy the right to the new node
        {
            newNode->keys[i-minmumNode-1] = keys[i];
            keys[i] = type();
            newNode->vals[i-minmumNode-1] = vals[i];
            vals[i] = offset_num();
        }
        newNode->next_leaf_node = this->next_leaf_node;
        this->next_leaf_node = newNode;
        
        newNode->parent = this->parent;
        newNode->count = minmumNode;
        this->count = minmumNode + 1;
    }       // end
    else if(!isLeaf)
    {
        key = keys[minmumNode];
        for(size_t i = minmumNode + 1;i < degree+1;i ++)
        {
            newNode->childs[i-minmumNode-1] = this->childs[i];
            newNode->childs[i-minmumNode-1]->parent = newNode;
            this->childs[i] = NULL;
        }
        for(size_t i = minmumNode + 1;i < degree;i ++)
        {
            newNode->keys[i-minmumNode-1] = this->keys[i];
            this->keys[i] = type();
        }
        this->keys[minmumNode] = type();
        newNode->parent = this->parent;
        newNode->count = minmumNode;
        this->count = minmumNode;
    }
    return newNode;
}


template <class type>
size_t node<type>::add(type &key)
{
    if(count == 0)
    {
        keys[0] = key;
        count ++;
        return 0;
    }
    else                    //count > 0
    {
        size_t index = 0;   // record the index of the tree
        bool exist = search(key, index);
        if(exist)
        {
            cout << "Error:In add(type &key),key has already in the tree!" << endl;
            exit(3);
        }
        else                // add the key into the node
        {
            for(size_t i = count;i > index;i --)
                keys[i] = keys[i-1];
            keys[index] = key;
            
            for(size_t i = count + 1;i > index+1;i --)
                childs[i] = childs[i-1];
            childs[index+1] = NULL;
            count ++;
            
            return index;
        }
    }
}


template <class type>
size_t node<type>::add(type &key,offset_num val)
{
    if(!isLeaf)
    {
        cout << "Error:add(type &key,offset_num val) is a function for leaf nodes" << endl;
        return -1;
    }
    if(count == 0)
    {
        keys[0] = key;
        vals[0] = val;
        count ++;
        return 0;
    }
    else //count > 0
    {
        size_t index = 0; // record the index of the tree
        bool exist = search(key, index);
        if(exist)
        {
            cout << "Error:In add(type &key, offset_num val),key has already in the tree!" << endl;
            exit(3);
        }
        else // add the key into the node
        {
            for(size_t i = count;i > index;i --)
            {
                keys[i] = keys[i-1];
                vals[i] = vals[i-1];
            }
            keys[index] = key;
            vals[index] = val;
            count ++;
            return index;
        }
    }
}

template <class type>
bool node<type>::remove_at(size_t index)
{
    if(index > count)
    {
        cout << "Error:In remove_at(size_t index), index is more than count!" << endl;
        return false;
    }
    else
    {
        if(isLeaf)
        {
            for(size_t i = index;i < count-1;i ++)
            {
                keys[i] = keys[i+1];
                vals[i] = vals[i+1];
            }
            keys[count-1] = type();
            vals[count-1] = offset_num();
        }
        else // nonleaf
        {
            for(size_t i = index;i < count-1;i ++)
                keys[i] = keys[i+1];
            
            for(size_t i = index+1;i < count;i ++)
                childs[i] = childs[i+1];
            
            keys[count-1] = type();
            childs[count] = NULL;
        }
        
        count --;
        return true;
    }
}


template <class type>
BPlusTree<type>::BPlusTree(string m_name,int keysize,int m_degree):file_name(m_name),keyCount(0),level(0),nodeCount(0),root(NULL),leafHead(NULL),keySize(keysize),file(NULL),degree(m_degree)
{
    init_tree();
    read_all_from_disk();
}


template <class type>
BPlusTree<type>:: ~BPlusTree()
{
    drop_tree(root);
    keyCount = 0;
    root = NULL;
    level = 0;
}


template <class type>
void BPlusTree<type>::init_tree()
{
    root = new node<type>(degree,true);
    keyCount = 0;
    level = 1;
    nodeCount = 1;
    leafHead = root;
}


template <class type>
void BPlusTree<type>::findToLeaf(Node pNode,type key,searchNodeParse & snp)
{
    size_t index = 0;
    if(pNode->search(key,index))    // find the key in the node
    {
        if(pNode->isLeaf)
        {
            snp.pNode = pNode;
            snp.index = index;
            snp.found_f = true;
        }
        else    // the node is not a leaf, continue searching
        {
            pNode = pNode -> childs[index + 1];
            while(!pNode->isLeaf)
            {
                pNode = pNode->childs[0];
            }
            snp.pNode = pNode;
            snp.index = 0;
            snp.found_f = true;
        }
        
    }
    else    // can not find the key
    {
        if(pNode->isLeaf)
        {
            snp.pNode = pNode;
            snp.index = index;
            snp.found_f = false;
        }
        else
        {
            findToLeaf(pNode->childs[index],key,snp);
        }
    }
}

template <class type>
bool BPlusTree<type>::insert_key(type &key,offset_num val)
{
    searchNodeParse snp;
    if(!root) init_tree();
    findToLeaf(root,key,snp);
    if(snp.found_f)
    {
        cout << "Error:in insert key to index: the duplicated key!" << endl;
        return false;
    }
    else
    {
        snp.pNode->add(key,val);
        if(snp.pNode->count == degree)
        {
            adjustAfterinsert(snp.pNode);
        }
        keyCount ++;
        return true;
    }
}

template <class type>
bool BPlusTree<type>::adjustAfterinsert(Node pNode)
{
    type key;
    Node newNode = pNode->splite(key);
    nodeCount ++;
    
    if(pNode->is_root())    // the node is root
    {
        Node root = new node<type>(degree,false);
        if(root == NULL)
        {
            cout << "Error: can not allocate memory for the new root in adjustAfterinsert" << endl;
            exit(1);
        }
        else
        {
            level ++;
            nodeCount ++;
            this->root = root;
            pNode->parent = root;
            newNode->parent = root;
            root->add(key);
            root->childs[0] = pNode;
            root->childs[1] = newNode;
            return true;
        }
    }       // end
    else    // if it is not the root
    {
        Node parent = pNode->parent;
        size_t index = parent->add(key);
        
        parent->childs[index+1] = newNode;
        newNode->parent = parent;
        if(parent->count == degree)
            return adjustAfterinsert(parent);
        
        return true;
    }
}


template <class type>
offset_num BPlusTree<type>::search(type& key)
{
    if(!root) return -1;
    searchNodeParse snp;
    findToLeaf(root, key, snp);
    if(!snp.found_f)
    {
        return -1;
    }
    else
    {
        return snp.pNode->vals[snp.index];
    }
    
}


template <class type>
bool BPlusTree<type>::delete_key(type &key)
{
    searchNodeParse snp;
    if(!root)
    {
        cout << "ERROR: In delete_key, no nodes in the tree " << file_name << "!" << endl;
        return false;
    }
    else
    {
        findToLeaf(root, key, snp);
        if(!snp.found_f)
        {
            cout << "ERROR: In delete_key, no keys in the tree " << file_name << "!" << endl;
            return false;
        }
        else // find the key
        {
            if(snp.pNode->is_root())
            {
                snp.pNode->remove_at(snp.index);
                keyCount --;
                return adjustAfterDelete(snp.pNode);
            }
            else
            {
                if(snp.index == 0 && leafHead != snp.pNode) // the key exist
                {
                    // update the branch level
                    size_t index = 0;
                    
                    Node now_parent = snp.pNode->parent;
                    bool if_found_inBranch = now_parent->search(key,index);
                    while(!if_found_inBranch)
                    {
                        if(now_parent->parent)
                            now_parent = now_parent->parent;
                        else
                        {
                            break;
                        }
                        if_found_inBranch = now_parent->search(key,index);
                    }// end of search
                    
                    now_parent -> keys[index] = snp.pNode->keys[1];
                    
                    snp.pNode->remove_at(snp.index);
                    keyCount--;
                    return adjustAfterDelete(snp.pNode);
                    
                }
                else //this key must just exist in the leaf too.
                {
                    snp.pNode->remove_at(snp.index);
                    keyCount--;
                    return adjustAfterDelete(snp.pNode);
                }
            }
        }
    }
}

//adjust the node after deletion
template <class type>
bool BPlusTree<type>::adjustAfterDelete(Node pNode)
{
    size_t minmumKey = (degree - 1) / 2;
    if(((pNode->isLeaf)&&(pNode->count >= minmumKey)) || ((degree != 3)&&(!pNode->isLeaf)&&(pNode->count >= minmumKey - 1)) || ((degree ==3)&&(!pNode->isLeaf)&&(pNode->count < 0))) // do not need to adjust
    {
        return  true;
    }
    if(pNode->is_root())
    {
        if(pNode->count > 0)    //no adjust
        {
            return true;
        }
        else
        {
            if(root->isLeaf)    //empty
            {
                delete pNode;
                root = NULL;
                leafHead = NULL;
                level --;
                nodeCount --;
            }
            else // root will be the leafhead
            {
                root = pNode -> childs[0];
                root -> parent = NULL;
                delete pNode;
                level --;
                nodeCount --;
            }
        }
    }
    else
    {
        Node parent = pNode->parent,brother = NULL;
        if(pNode->isLeaf)
        {
            size_t index = 0;
            parent->search(pNode->keys[0],index);
            
            if((parent->childs[0] != pNode) && (index + 1 == parent->count))
            {
                brother = parent->childs[index];
                if(brother->count > minmumKey)
                {
                    for(size_t i = pNode->count;i > 0;i --)
                    {
                        pNode->keys[i] = pNode->keys[i-1];
                        pNode->vals[i] = pNode->vals[i-1];
                    }
                    pNode->keys[0] = brother->keys[brother->count-1];
                    pNode->vals[0] = brother->vals[brother->count-1];
                    brother->remove_at(brother->count-1);
                    
                    pNode->count ++;
                    parent->keys[index] = pNode->keys[0];
                    return true;
                    
                }
                else    //merge
                {
                    parent->remove_at(index);
                    
                    for(int i = 0;i < pNode->count;i ++)
                    {
                        brother->keys[i+brother->count] = pNode->keys[i];
                        brother->vals[i+brother->count] = pNode->vals[i];
                    }
                    brother->count += pNode->count;
                    brother->next_leaf_node = pNode->next_leaf_node;
                    
                    delete pNode;
                    nodeCount --;
                    
                    return adjustAfterDelete(parent);
                }// end merge
                
            }
            else
            {
                if(parent->childs[0] == pNode)
                    brother = parent->childs[1];
                else
                    brother = parent->childs[index+2];
                if(brother->count > minmumKey)
                {
                    pNode->keys[pNode->count] = brother->keys[0];
                    pNode->vals[pNode->count] = brother->vals[0];
                    pNode->count ++;
                    brother->remove_at(0);
                    if(parent->childs[0] == pNode)
                        parent->keys[0] = brother->keys[0];
                    else
                        parent->keys[index+1] = brother->keys[0];
                    return true;
                    
                }// end add
                else // merge the node with brother
                {
                    for(int i = 0;i < brother->count;i ++)
                    {
                        pNode->keys[pNode->count+i] = brother->keys[i];
                        pNode->vals[pNode->count+i] = brother->vals[i];
                    }
                    if(pNode == parent->childs[0])
                        parent->remove_at(0);
                    else
                        parent->remove_at(index+1);
                    pNode->count += brother->count;
                    pNode->next_leaf_node = brother->next_leaf_node;
                    delete brother;
                    nodeCount --;
                    
                    return adjustAfterDelete(parent);
                }   //end merge
            }       // end of the right brother
            
        }           // end leaf
        else        // branch
        {
            size_t index = 0;
            parent->search(pNode->childs[0]->keys[0],index);
            if((parent->childs[0] != pNode) && (index + 1 == parent->count))
            {
                brother = parent->childs[index];
                if(brother->count > minmumKey - 1)
                {
                    pNode->childs[pNode->count+1] = pNode->childs[pNode->count];
                    for(size_t i = pNode->count;i > 0;i --)
                    {
                        pNode->childs[i] = pNode->childs[i-1];
                        pNode->keys[i] = pNode->keys[i-1];
                    }
                    pNode->childs[0] = brother->childs[brother->count];
                    pNode->keys[0] = parent->keys[index];
                    pNode->count ++;
                    //modify father
                    parent->keys[index]= brother->keys[brother->count-1];
                    //modify brother and child
                    if(brother->childs[brother->count])
                    {
                        brother->childs[brother->count]->parent = pNode;
                    }
                    brother->remove_at(brother->count-1);
                    
                    return true;
                    
                }       // end add
                else    // merge the node with its brother
                {
                    brother->keys[brother->count] = parent->keys[index];
                    parent->remove_at(index);
                    brother->count ++;
                    
                    for(int i = 0;i < pNode->count;i ++)
                    {
                        brother->childs[brother->count+i] = pNode->childs[i];
                        brother->keys[brother->count+i] = pNode->keys[i];
                        brother->childs[brother->count+i]-> parent= brother;
                    }
                    brother->childs[brother->count+pNode->count] = pNode->childs[pNode->count];
                    brother->childs[brother->count+pNode->count]->parent = brother;
                    
                    brother->count += pNode->count;
                    
                    
                    delete pNode;
                    nodeCount --;
                    
                    return adjustAfterDelete(parent);
                }
                
            
            }
            else // choose the right brother
            {
                if(parent->childs[0] == pNode)
                    brother = parent->childs[1];
                else
                    brother = parent->childs[index+2];
                if(brother->count > minmumKey - 1)
                {
                    pNode->childs[pNode->count+1] = brother->childs[0];
                    pNode->keys[pNode->count] = brother->keys[0];
                    pNode->childs[pNode->count+1]->parent = pNode;
                    pNode->count ++;
                    //modify the fater
                    if(pNode == parent->childs[0])
                        parent->keys[0] = brother->keys[0];
                    else
                        parent->keys[index+1] = brother->keys[0];
                    //modify the brother
                    brother->childs[0] = brother->childs[1];
                    brother->remove_at(0);
                    
                    return true;
                }
                else // merge the node with its brother
                {
                    //modify the pnode and child
                    pNode->keys[pNode->count] = parent->keys[index];
                    
                    if(pNode == parent->childs[0])
                        parent->remove_at(0);
                    else
                        parent->remove_at(index+1);
                    
                    pNode->count ++;
                    
                    for(int i = 0;i < brother->count;i++)
                    {
                        pNode->childs[pNode->count+i] = brother->childs[i];
                        pNode->keys[pNode->count+i] = brother->keys[i];
                        pNode->childs[pNode->count+i]->parent = pNode;
                    }
                    pNode->childs[pNode->count+brother->count] = brother->childs[brother->count];
                    pNode->childs[pNode->count+brother->count]->parent = pNode;
                    
                    pNode->count += brother->count;
                    
                    
                    delete brother;
                    nodeCount --;
                    
                    return adjustAfterDelete(parent);
                    
                }
                
            }
            
        }
        
    }
    return false;
}

//drop the tree
template <class type>
void BPlusTree<type>::drop_tree(Node node)
{
    if(!node) return;
    if(!node->isLeaf) //if it has child
    {
        for(size_t i=0;i <= node->count;i++)
        {
            drop_tree(node->childs[i] );
            node->childs[i] = NULL;
        }
    }
    delete node;
    nodeCount --;
    return;
}

//read the tree from the disk
template <class type>
void BPlusTree<type>::read_all_from_disk()
{
    file = bm.getFile(file_name.c_str());
    blockNode* btmp = bm.get_block_head(file);
    while (true)
    {
        if (btmp == NULL)
        {
            return;
        }
        
        read_from_disk(btmp);
        if(btmp->ifbottom) break;
        btmp = bm.getNextBlock(file, btmp);
    }
    
}

//read a node from the disk
template <class type>
void BPlusTree<type>::read_from_disk(blockNode* btmp)
{
    int valueSize = sizeof(offset_num);
    char* indexBegin = bm.get_content(*btmp);
    char* valueBegin = indexBegin + keySize;
    type key;
    offset_num value;
    
    while(valueBegin - bm.get_content(*btmp) < bm.get_using_size(*btmp))
        // there are available position in the block
    {
        key = *(type*)indexBegin;
        value = *(offset_num*)valueBegin;
        insert_key(key, value);
        valueBegin += keySize + valueSize;
        indexBegin += keySize + valueSize;
    }
    
}

//write tree data to the disk
template <class type>
void BPlusTree<type>::write_back_all_to_disk()
{
    blockNode* btmp = bm.get_block_head(file);
    Node ntmp = leafHead;
    int valueSize = sizeof(offset_num);
    while(ntmp != NULL)
    {
        bm.set_using_size(*btmp, 0);
        bm.set_dirty(*btmp);
        for(int i = 0;i < ntmp->count;i ++)
        {
            char* key = (char*)&(ntmp->keys[i]);
            char* value = (char*)&(ntmp->vals[i]);
            memcpy(bm.get_content(*btmp)+bm.get_using_size(*btmp),key,keySize);
            bm.set_using_size(*btmp, bm.get_using_size(*btmp) + keySize);
            memcpy(bm.get_content(*btmp)+bm.get_using_size(*btmp),value,valueSize);
            bm.set_using_size(*btmp, bm.get_using_size(*btmp) + valueSize);
        }
        
        btmp = bm.getNextBlock(file, btmp);
        ntmp = ntmp->next_leaf_node;
    }
    while(1)// delete the empty node
    {
        if(btmp->ifbottom)
            break;
        bm.set_using_size(*btmp, 0);
        bm.set_dirty(*btmp);
        btmp = bm.getNextBlock(file, btmp);
    }
    
}



#endif