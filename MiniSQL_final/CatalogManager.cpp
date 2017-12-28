#include "CatalogManager.h"
#include "BufferManager.h"
#include "Indexinfo.h"
#include <iostream>
#include <vector>
#include <string>
#include <cstring>
#include <sstream>
#include "Attribute.h"
#define UNKNOWN_FILE 8
#define TABLE_FILE 9
#define INDEX_FILE 10
CatalogManager::CatalogManager() {
}

CatalogManager::~CatalogManager() {
}

int CatalogManager::drop_table(string tableName)
{
    bm.delete_fileNode(tableName.c_str());
    if (remove(tableName.c_str()))
    {
        return 0;
    }
    return 1;
}

int CatalogManager::get_index_type(string indexName)
{
    fileNode *ftmp = bm.getFile("Indexs");
    blockNode *btmp = bm.get_block_head(ftmp);
    if (btmp )
    {
        char* addressBegin;
        addressBegin = bm.get_content(*btmp);
        index_info * i = (index_info *)addressBegin;
        for(int j = 0 ;j<(bm.get_using_size(*btmp)/sizeof(index_info));j++)
        {
            if((*i).index_name==indexName)
            {
                return i->type;
            }
            i ++;
        }
        return -2;
    }

    return -2;
}

int CatalogManager::get_index_all(vector<index_info> * indexs)
{
    fileNode *ftmp = bm.getFile("Indexs");
    blockNode *btmp = bm.get_block_head(ftmp);
    if (btmp )
    {
        char* addressBegin;
        addressBegin = bm.get_content(*btmp);
        index_info * i = (index_info *)addressBegin;
        for(int j = 0 ;j<(bm.get_using_size(*btmp)/sizeof(index_info));j++)
        {
            indexs->push_back((*i));
            i ++;
        }
    }

    return 1;
}
int CatalogManager::add_index(string indexName,string tableName,string Attribute,int type)
{
    fileNode *ftmp = bm.getFile("Indexs");
    blockNode *btmp = bm.get_block_head(ftmp);
    index_info i(indexName,tableName,Attribute,type);
    
    while (true)
    {
        if (btmp == NULL)
        {
            return 0;
        }
        if (bm.get_using_size(*btmp) <= bm.get_block_size() - sizeof(index_info))
        {

            char* addressBegin;
            addressBegin = bm.get_content(*btmp) + bm.get_using_size(*btmp);
            memcpy(addressBegin, &i, sizeof(index_info));
            bm.set_using_size(*btmp, bm.get_using_size(*btmp) + sizeof(index_info));
            bm.set_dirty(*btmp);

            return this->set_index_on_attr(tableName,Attribute,indexName);
        }
        else
        {
            btmp = bm.getNextBlock(ftmp, btmp);
        }
    }

    return 0;
}
int CatalogManager::find_table(string tableName)
{
    FILE *fp;
    fp = fopen(tableName.c_str(), "r");
    if (fp == NULL)
    {
        return 0;
    }
    else
    {
        fclose(fp);
        return TABLE_FILE;
    }

}
int CatalogManager::find_index(string fileName)
{
    fileNode *ftmp = bm.getFile("Indexs");
    blockNode *btmp = bm.get_block_head(ftmp);
    if (btmp )
    {
        char* addressBegin;
        addressBegin = bm.get_content(*btmp);
        index_info * i = (index_info *)addressBegin;
        int flag = UNKNOWN_FILE;
        for(int j = 0 ;j<(bm.get_using_size(*btmp)/sizeof(index_info));j++)
        {
            if((*i).index_name==fileName)
            {
                flag = INDEX_FILE;
                break;
            }
            i ++;
        }
        return flag;
    }

    return 0;
}
int CatalogManager::drop_index(string index)
{
    fileNode *ftmp = bm.getFile("Indexs");
    blockNode *btmp = bm.get_block_head(ftmp);
    if (btmp)
    {
        char* addressBegin;
        addressBegin = bm.get_content(*btmp);
        index_info * i = (index_info *)addressBegin;
        int j = 0;
        for(j = 0 ;j<(bm.get_using_size(*btmp)/sizeof(index_info));j++)
        {
            if((*i).index_name==index)
            {
                break;
            }
            i ++;
        }
        this->revoke_index_on_attr((*i).table_name,(*i).Attribute,(*i).index_name);
        for (;j<(bm.get_using_size(*btmp)/sizeof(index_info)-1);j++)
        {
            (*i) = *(i + sizeof(index_info));
            i ++;
        }
        bm.set_using_size(*btmp, bm.get_using_size(*btmp) - sizeof(index_info));
        bm.set_dirty(*btmp);

        return 1;
    }

    return 0;
}
int CatalogManager::revoke_index_on_attr(string tableName,string AttributeName,string indexName)
{
    fileNode *ftmp = bm.getFile(tableName.c_str());
    blockNode *btmp = bm.get_block_head(ftmp);

    if (btmp)
    {

        char* addressBegin = bm.get_content(*btmp) ;
        addressBegin += (1+sizeof(int));
        int size = *addressBegin;
        addressBegin++;
        Attribute *a = (Attribute *)addressBegin;
        int i;
        for(i =0;i<size;i++)
        {
            if(a->name == AttributeName)
            {
                if(a->index == indexName)
                {
                    a->index = "";
                    bm.set_dirty(*btmp);
                }
                else
                {
                    cout<<"revoke unknown index: "<<indexName<<" on "<<tableName<<"!"<<endl;
                    cout<<"Attribute: "<<AttributeName<<" on table "<<tableName<<" has index: "<<a->index<<"!"<<endl;
                }
                break;
            }
            a ++;
        }
        if(i<size)
            return 1;
        else
            return 0;
    }
    return 0;
}
int CatalogManager::get_index_name_list(string tableName, vector<string>* indexNameVector)
{
    fileNode *ftmp = bm.getFile("Indexs");
    blockNode *btmp = bm.get_block_head(ftmp);
    if (btmp )
    {
        char* addressBegin;
        addressBegin = bm.get_content(*btmp);
        index_info * i = (index_info *)addressBegin;
        for(int j = 0 ;j<(bm.get_using_size(*btmp)/sizeof(index_info));j++)
        {
            if((*i).table_name==tableName)
            {
                (*indexNameVector).push_back((*i).index_name);
            }
            i ++;
        }
        return 1;
    }

    return 0;
}

int CatalogManager::delete_value(string tableName, int deleteNum)
{
    fileNode *ftmp = bm.getFile(tableName.c_str());
    blockNode *btmp = bm.get_block_head(ftmp);

    if (btmp)
    {

        char* addressBegin = bm.get_content(*btmp) ;
        int * recordNum = (int*)addressBegin;
        if((*recordNum) <deleteNum)
        {
            cout<<"error in CatalogManager::deleteValue"<<endl;
            return 0;
        }
        else
            (*recordNum) -= deleteNum;

        bm.set_dirty(*btmp);
        return *recordNum;
    }
    return 0;
}
int CatalogManager::insert_record(string tableName, int recordNum)
{
    fileNode *ftmp = bm.getFile(tableName.c_str());
    blockNode *btmp = bm.get_block_head(ftmp);

    if (btmp)
    {

        char* addressBegin = bm.get_content(*btmp) ;
        int * originalRecordNum = (int*)addressBegin;
        *originalRecordNum += recordNum;
        bm.set_dirty(*btmp);
        return *originalRecordNum;
    }
    return 0;
}

int CatalogManager::get_record_num(string tableName)
{
    fileNode *ftmp = bm.getFile(tableName.c_str());
    blockNode *btmp = bm.get_block_head(ftmp);

    if (btmp)
    {
        char* addressBegin = bm.get_content(*btmp) ;
        int * recordNum = (int*)addressBegin;
        return *recordNum;
    }
    return 0;
}

int CatalogManager::add_table(string tableName, vector<Attribute>* attributeVector, string primaryKeyName = "",int primaryKeyLocation = 0)
{
    FILE *fp;
    fp = fopen(tableName.c_str(), "w+");
    if (fp == NULL)
    {
        return 0;
    }
    fclose(fp);
    fileNode *ftmp = bm.getFile(tableName.c_str());
    blockNode *btmp = bm.get_block_head(ftmp);

    if (btmp )
    {
        char* addressBegin = bm.get_content(*btmp) ;
        int * size = (int*)addressBegin;
        *size = 0;// 0 record number
        addressBegin += sizeof(int);
        *addressBegin = primaryKeyLocation;//1 as what it says
        addressBegin++;
        *addressBegin = (*attributeVector).size();// 2 attribute number
        addressBegin++;
        //memcpy(addressBegin, attributeVector, (*attributeVector).size()*sizeof(Attribute));
        for(int i= 0;i<(*attributeVector).size();i++)
        {
            memcpy(addressBegin, &((*attributeVector)[i]), sizeof(Attribute));
            addressBegin += sizeof(Attribute);
        }
        bm.set_using_size(*btmp, bm.get_using_size(*btmp) + (*attributeVector).size()*sizeof(Attribute)+2+sizeof(int));
        bm.set_dirty(*btmp);
        return 1;
    }
    return 0;
}
int CatalogManager::set_index_on_attr(string tableName,string AttributeName,string indexName)
{
    fileNode *ftmp = bm.getFile(tableName.c_str());
    blockNode *btmp = bm.get_block_head(ftmp);

    if (btmp)
    {

        char* addressBegin = bm.get_content(*btmp) ;
        addressBegin += 1+sizeof(int);
        int size = *addressBegin;
        addressBegin++;
        Attribute *a = (Attribute *)addressBegin;
        int i;
        for(i =0;i<size;i++)
        {
            if(a->name == AttributeName)
            {
                a->index = indexName;
                bm.set_dirty(*btmp);
                break;
            }
            a ++;
        }
        if(i<size)
            return 1;
        else
            return 0;
    }
    return 0;
}
int CatalogManager::get_attribute(string tableName, vector<Attribute>* attributeVector)
{
    fileNode *ftmp = bm.getFile(tableName.c_str());
    blockNode *btmp = bm.get_block_head(ftmp);

    if (btmp)
    {

        char* addressBegin = bm.get_content(*btmp) ;
        addressBegin += 1+sizeof(int);
        int size = *addressBegin;
        addressBegin++;
        Attribute *a = (Attribute *)addressBegin;
        for(int i =0;i<size;i++)
        {
            attributeVector->push_back(*a);
            a ++;
        }

        return 1;
    }
    return 0;
}

int CatalogManager::cal_length(string tableName)
{
    fileNode *ftmp = bm.getFile(tableName.c_str());
    blockNode *btmp = bm.get_block_head(ftmp);

    if (btmp)
    {
        int singleRecordSize =  0;
        char* addressBegin = bm.get_content(*btmp) ;
        addressBegin += 1+sizeof(int);
        int size = *addressBegin;
        addressBegin++;
        Attribute *a = (Attribute *)addressBegin;
        for(int i =0;i<size;i++)
        {
            if((*a).type==-1)
            {
                singleRecordSize += sizeof(float);
            }
            else if((*a).type == 0)
            {
                singleRecordSize += sizeof(int);
            }
            else if((*a).type>0)
            {
                singleRecordSize += (*a).type * sizeof(char);
            }
            else
            {
                cout<<"Catalog data damaged!"<<endl;
                return 0;
            }
            a ++;
        }

        return singleRecordSize;
    }
    return 0;
}

int CatalogManager::cal_length2(int type)
{
    if (type == Attribute::TYPE_INT) {
        return sizeof(int);
    }
    else if (type == Attribute::TYPE_FLOAT)
    {
        return sizeof(float);
    }
    else{
        return (int) sizeof(char[type]); // Note that the type stores in Attribute.h
    }
}

void CatalogManager::get_record_string(string tableName, vector<string>* recordContent, char* recordResult)
{
    vector<Attribute> attributeVector;
    get_attribute(tableName, &attributeVector);
    char * contentBegin = recordResult;

    for(int i = 0; i < attributeVector.size(); i++)
    {
        Attribute attribute = attributeVector[i];
        string content = (*recordContent)[i];
        int type = attribute.type;
        int typeSize = cal_length2(type);
        stringstream ss;
        ss << content;
        if (type == Attribute::TYPE_INT)
        {
            //if the content is a int
            int intTmp;
            ss >> intTmp;
            memcpy(contentBegin, ((char*)&intTmp), typeSize);
        }
        else if (type == Attribute::TYPE_FLOAT)
        {
            //if the content is a float
            float floatTmp;
            ss >> floatTmp;
            memcpy(contentBegin, ((char*)&floatTmp), typeSize);
        }
        else
        {
            memcpy(contentBegin, content.c_str(), typeSize);
        }

        contentBegin += typeSize;
    }
    return ;
}
