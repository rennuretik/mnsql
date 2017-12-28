#include "IndexManager.h"
#include <iostream>
#include "API.h"
#include "IndexInfo.h"
#include <vector>
using namespace std;



IndexManager::IndexManager(API *m_api)
{
    api = m_api;
    vector<index_info> allIndexInfo;
    api->get_all_index_addr_info(&allIndexInfo);
    for(vector<index_info>::iterator i = allIndexInfo.begin();i != allIndexInfo.end();i ++)
    {
        create_index(i->index_name, i->type);
    }
}


IndexManager::~IndexManager()
{
    //write back to the disk
    for(intMap::iterator itInt = indexIntMap.begin();itInt != indexIntMap.end(); itInt ++)
    {
        if(itInt->second)
        {
            itInt -> second->write_back_all_to_disk();
            delete itInt->second;
        }
    }
    for(stringMap::iterator itString = indexStringMap.begin();itString != indexStringMap.end(); itString ++)
    {
        if(itString->second)
        {
            itString ->second-> write_back_all_to_disk();
            delete itString->second;
        }
    }
    for(floatMap::iterator itFloat = indexFloatMap.begin();itFloat != indexFloatMap.end(); itFloat ++)
    {
        if(itFloat->second)
        {
            itFloat ->second-> write_back_all_to_disk();
            delete itFloat->second;
        }
    }
}



void IndexManager::create_index(string filePath,int type)
{
    int keySize = get_key_size(type);
    int degree = get_degree(type);
    if(type == TYPE_INT)
    {
        BPlusTree<int> *tree = new BPlusTree<int>(filePath,keySize,degree);
        indexIntMap.insert(intMap::value_type(filePath, tree));
    }
    else if(type == TYPE_FLOAT)
    {
        BPlusTree<float> *tree = new BPlusTree<float>(filePath,keySize,degree);
        indexFloatMap.insert(floatMap::value_type(filePath, tree));
    }
    else // string
    {
        BPlusTree<string> *tree = new BPlusTree<string>(filePath,keySize,degree);
        indexStringMap.insert(stringMap::value_type(filePath, tree));
    }
    
}


void IndexManager::drop_index(string filePath,int type)
{
    if(type == TYPE_INT)
    {
        intMap::iterator itInt = indexIntMap.find(filePath);
        if(itInt == indexIntMap.end())
        {
            cout << "Error:in drop index, no index " << filePath <<" exits" << endl;
            return;
        }
        else
        {
            delete itInt->second;
            indexIntMap.erase(itInt);
        }
    }
    else if(type == TYPE_FLOAT)
    {
        floatMap::iterator itFloat = indexFloatMap.find(filePath);
        if(itFloat == indexFloatMap.end())
        {
            cout << "Error:in drop index, no index " << filePath <<" exits" << endl;
            return;
        }
        else
        {
            delete itFloat->second;
            indexFloatMap.erase(itFloat);
        }
    }
    else // string
    {
        stringMap::iterator itString = indexStringMap.find(filePath);
        if(itString == indexStringMap.end())
        {
            cout << "Error:in drop index, no index " << filePath <<" exits" << endl;
            return;
        }
        else
        {
            delete itString->second;
            indexStringMap.erase(itString);
        }
    }

}


offset_num IndexManager::search_index(string filePath,string key,int type)
{
    set_key(type, key);
    
    if(type == TYPE_INT)
    {
        intMap::iterator itInt = indexIntMap.find(filePath);
        if(itInt == indexIntMap.end())
        {
            cout << "Error:in search index, no index " << filePath <<" exits" << endl;
            return -1;
        }
        else
        {
            return itInt->second->search(kt.intTmp);
        }
    }
    else if(type == TYPE_FLOAT)
    {
        floatMap::iterator itFloat = indexFloatMap.find(filePath);
        if(itFloat == indexFloatMap.end())
        {
            cout << "Error:in search index, no index " << filePath <<" exits" << endl;
            return -1;
        }
        else
        {
            return itFloat->second->search(kt.floatTmp);

        }
    }
    else // string
    {
        stringMap::iterator itString = indexStringMap.find(filePath);
        if(itString == indexStringMap.end())
        {
            cout << "Error:in search index, no index " << filePath <<" exits" << endl;
            return -1;
        }
        else
        {
            return itString->second->search(key);
        }
    }
}


void IndexManager::insert_index(string filePath,string key,offset_num blockOffset,int type)
{
    set_key(type, key);

    if(type == TYPE_INT)
    {
        intMap::iterator itInt = indexIntMap.find(filePath);
        if(itInt == indexIntMap.end())
        {
            cout << "Error:in search index, no index " << filePath <<" exits" << endl;
            return;
        }
        else
        {
            itInt->second->insert_key(kt.intTmp,blockOffset);
        }
    }
    else if(type == TYPE_FLOAT)
    {
        floatMap::iterator itFloat = indexFloatMap.find(filePath);
        if(itFloat == indexFloatMap.end())
        {
            cout << "Error:in search index, no index " << filePath <<" exits" << endl;
            return;
        }
        else
        {
            itFloat->second->insert_key(kt.floatTmp,blockOffset);
            
        }
    }
    else // string
    {
        stringMap::iterator itString = indexStringMap.find(filePath);
        if(itString == indexStringMap.end())
        {
            cout << "Error:in search index, no index " << filePath <<" exits" << endl;
            return;
        }
        else
        {
            itString->second->insert_key(key,blockOffset);
        }
    }
}


void IndexManager::delete_index_by_key(string filePath,string key,int type)
{
    set_key(type, key);

    if(type == TYPE_INT)
    {
        intMap::iterator itInt = indexIntMap.find(filePath);
        if(itInt == indexIntMap.end())
        {
            cout << "Error:in search index, no index " << filePath <<" exits" << endl;
            return;
        }
        else
        {
            itInt->second->delete_key(kt.intTmp);
        }
    }
    else if(type == TYPE_FLOAT)
    {
        floatMap::iterator itFloat = indexFloatMap.find(filePath);
        if(itFloat == indexFloatMap.end())
        {
            cout << "Error:in search index, no index " << filePath <<" exits" << endl;
            return;
        }
        else
        {
            itFloat->second->delete_key(kt.floatTmp);
            
        }
    }
    else // string
    {
        stringMap::iterator itString = indexStringMap.find(filePath);
        if(itString == indexStringMap.end())
        {
            cout << "Error:in search index, no index " << filePath <<" exits" << endl;
            return;
        }
        else
        {
            itString->second->delete_key(key);
        }
    }
}


int IndexManager::get_degree(int type)
{
    int degree = bm.get_block_size()/(get_key_size(type)+sizeof(offset_num));
    if(degree %2 == 0) degree -= 1;
    return degree;
}


int IndexManager::get_key_size(int type)
{
    if(type == TYPE_FLOAT)
        return sizeof(float);
    else if(type == TYPE_INT)
        return sizeof(int);
    else if(type > 0)
        return type + 1;
    else
    {
        cout << "ERROR: in get_key_size: invalid type" << endl;
        return -100;
    }
}


void IndexManager::set_key(int type,string key)
{
    stringstream ss;
    ss << key;
    if(type == this->TYPE_INT)
        ss >> this->kt.intTmp;
    else if(type == this->TYPE_FLOAT)
        ss >> this->kt.floatTmp;
    else if(type > 0)
        ss >> this->kt.stringTmp;
    else
        cout << "Error: in getKey: invalid type" << endl;

}






