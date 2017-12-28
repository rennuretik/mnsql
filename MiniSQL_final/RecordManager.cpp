#include <iostream>
#include "RecordManager.h"
#include <cstring>
#include "API.h"

//create a table
int RecordManager::create_table(string tableName)
{
    string tableFileName = get_file_name_table(tableName);
    
    FILE *fp;
    fp = fopen(tableFileName.c_str(), "w+");
    if (fp == NULL)
    {
        return 0;
    }
    fclose(fp);
    return 1;
}


int RecordManager::drop_table(string tableName)
{
    string tableFileName = get_file_name_table(tableName);
    bm.delete_fileNode(tableFileName.c_str());
    if (remove(tableFileName.c_str()))
    {
        return 0;
    }
    return 1;
}


int RecordManager::create_index(string indexName)
{
    string indexFileName = get_file_name_index(indexName);
    
    FILE *fp;
    fp = fopen(indexFileName.c_str(), "w+");
    if (fp == NULL)
    {
        return 0;
    }
    fclose(fp);
    return 1;
}


int RecordManager::drop_index(string indexName)
{
    string indexFileName = get_file_name_index(indexName);
    bm.delete_fileNode(indexFileName.c_str());
    if (remove(indexFileName.c_str()))
    {
        return 0;
    }
    return 1;
}


int RecordManager::insert_record(string tableName,char* record, int recordSize)
{
    fileNode *ftmp = bm.getFile(get_file_name_table(tableName).c_str());
    blockNode *btmp = bm.get_block_head(ftmp);
    while (true)
    {
        if (btmp == NULL)
        {
            return -1;
        }
        if (bm.get_using_size(*btmp) <= bm.get_block_size() - recordSize)
        {
            
            char* addressBegin;
            addressBegin = bm.get_content(*btmp) + bm.get_using_size(*btmp);
            memcpy(addressBegin, record, recordSize);
            bm.set_using_size(*btmp, bm.get_using_size(*btmp) + recordSize);
            bm.set_dirty(*btmp);
            return btmp->offsetNum;
        }
        else
        {
            btmp = bm.getNextBlock(ftmp, btmp);
        }
    }
    
    return -1;
}


int RecordManager::show_all_records(string tableName, vector<string>* attributeNameVector,  vector<Condition>* conditionVector)
{
    fileNode *ftmp = bm.getFile(get_file_name_table(tableName).c_str());
    blockNode *btmp = bm.get_block_head(ftmp);
    int count = 0;
    while (true)
    {
        if (btmp == NULL)
        {
            return -1;
        }
        if (btmp->ifbottom)
        {
            int recordBlockNum = show_block_record(tableName,attributeNameVector, conditionVector, btmp);
            count += recordBlockNum;
            return count;
        }
        else
        {
            int recordBlockNum = show_block_record(tableName, attributeNameVector, conditionVector, btmp);
            count += recordBlockNum;
            btmp = bm.getNextBlock(ftmp, btmp);
        }
    }
    
    return -1;
}


int RecordManager::show_block_record(string tableName, vector<string>* attributeNameVector, vector<Condition>* conditionVector, int blockOffset)
{
    fileNode *ftmp = bm.getFile(get_file_name_table(tableName).c_str());
    blockNode* block = bm.getBlockByOffset(ftmp, blockOffset);
    if (block == NULL)
    {
        return -1;
    }
    else
    {
        return  show_block_record(tableName, attributeNameVector, conditionVector, block);
    }
}


int RecordManager::show_block_record(string tableName, vector<string>* attributeNameVector, vector<Condition>* conditionVector, blockNode* block)
{
    
    //if block is null, return -1
    if (block == NULL)
    {
        return -1;
    }
    
    int count = 0;
    
    char* recordBegin = bm.get_content(*block);
    vector<Attribute> attributeVector;
    int recordSize = api->get_records_size(tableName);

    api->get_attribute(tableName, &attributeVector);
    char* blockBegin = bm.get_content(*block);
    size_t usingSize = bm.get_using_size(*block);
    
    while (recordBegin - blockBegin  < usingSize)
    {
        //if the recordBegin point to a record
        
        if(fit_record_condition(recordBegin, recordSize, &attributeVector, conditionVector))
        {
            count ++;
            print_record(recordBegin, recordSize, &attributeVector, attributeNameVector);
            printf("\n");
        }
        
        recordBegin += recordSize;
    }
    
    return count;
}


int RecordManager::find_all_record(string tableName, vector<Condition>* conditionVector)
{
    fileNode *ftmp = bm.getFile(get_file_name_table(tableName).c_str());
    blockNode *btmp = bm.get_block_head(ftmp);
    int count = 0;
    while (true)
    {
        if (btmp == NULL)
        {
            return -1;
        }
        if (btmp->ifbottom)
        {
            int recordBlockNum = find_block_record(tableName, conditionVector, btmp);
            count += recordBlockNum;
            return count;
        }
        else
        {
            int recordBlockNum = find_block_record(tableName, conditionVector, btmp);
            count += recordBlockNum;
            btmp = bm.getNextBlock(ftmp, btmp);
        }
    }
    
    return -1;
}

int RecordManager::find_block_record(string tableName, vector<Condition>* conditionVector, blockNode* block)
{
    //if block is null, return -1
    if (block == NULL)
    {
        return -1;
    }
    int count = 0;
    
    char* recordBegin = bm.get_content(*block);
    vector<Attribute> attributeVector;
    int recordSize = api->get_records_size(tableName);
    
    api->get_attribute(tableName, &attributeVector);
    
    while (recordBegin - bm.get_content(*block)  < bm.get_using_size(*block))
    {
        if(fit_record_condition(recordBegin, recordSize, &attributeVector, conditionVector))
        {
            count++;
        }
        
        recordBegin += recordSize;
        
    }
    
    return count;
}


int RecordManager::delete_all_record(string tableName, vector<Condition>* conditionVector)
{
    fileNode *ftmp = bm.getFile(get_file_name_table(tableName).c_str());
    blockNode *btmp = bm.get_block_head(ftmp);

    int count = 0;
    while (true)
    {
        if (btmp == NULL)
        {
            return -1;
        }
        if (btmp->ifbottom)
        {
            int recordBlockNum = delete_block_record(tableName, conditionVector, btmp);
            count += recordBlockNum;
            return count;
        }
        else
        {
            int recordBlockNum = delete_block_record(tableName, conditionVector, btmp);
            count += recordBlockNum;
            btmp = bm.getNextBlock(ftmp, btmp);
        }
    }
    
    return -1;
}


int RecordManager::delete_block_record(string tableName,  vector<Condition>* conditionVector, int blockOffset)
{
    fileNode *ftmp = bm.getFile(get_file_name_table(tableName).c_str());
    blockNode* block = bm.getBlockByOffset(ftmp, blockOffset);
    if (block == NULL)
    {
        return -1;
    }
    else
    {
        return  delete_block_record(tableName, conditionVector, block);
    }
}


int RecordManager::delete_block_record(string tableName,  vector<Condition>* conditionVector, blockNode* block)
{
    //if block is null, return -1
    if (block == NULL)
    {
        return -1;
    }
    int count = 0;
    
    char* recordBegin = bm.get_content(*block);
    vector<Attribute> attributeVector;
    int recordSize = api->get_records_size(tableName);
    
    api->get_attribute(tableName, &attributeVector);
    
    while (recordBegin - bm.get_content(*block) < bm.get_using_size(*block))
    {
        if(fit_record_condition(recordBegin, recordSize, &attributeVector, conditionVector))
        {
            count ++;
            
            api->delete_record_index(recordBegin, recordSize, &attributeVector, block->offsetNum);
            int i = 0;
            for (i = 0; i + recordSize + recordBegin - bm.get_content(*block) < bm.get_using_size(*block); i++)
            {
                recordBegin[i] = recordBegin[i + recordSize];
            }
            memset(recordBegin + i, 0, recordSize);
            bm.set_using_size(*block, bm.get_using_size(*block) - recordSize);
            bm.set_dirty(*block);
        }
        else
        {
            recordBegin += recordSize;
        }
    }
    
    return count;
}


int RecordManager::all_index_records_already_insert(string tableName,string indexName)
{
    fileNode *ftmp = bm.getFile(get_file_name_table(tableName).c_str());
    blockNode *btmp = bm.get_block_head(ftmp);
    int count = 0;
    while (true)
    {
        if (btmp == NULL)
        {
            return -1;
        }
        if (btmp->ifbottom)
        {
            int recordBlockNum = index_block_record_already_insert(tableName, indexName, btmp);
            count += recordBlockNum;
            return count;
        }
        else
        {
            int recordBlockNum = index_block_record_already_insert(tableName, indexName, btmp);
            count += recordBlockNum;
            btmp = bm.getNextBlock(ftmp, btmp);
        }
    }
    
    return -1;
}



 int RecordManager::index_block_record_already_insert(string tableName,string indexName,  blockNode* block)
{
    if (block == NULL)
    {
        return -1;
    }
    int count = 0;
    
    char* recordBegin = bm.get_content(*block);
    vector<Attribute> attributeVector;
    int recordSize = api->get_records_size(tableName);
    
    api->get_attribute(tableName, &attributeVector);
    
    int type;
    int typeSize;
    char * contentBegin;
    
    while (recordBegin - bm.get_content(*block)  < bm.get_using_size(*block))
    {
        contentBegin = recordBegin;
        //if the recordBegin point to a record
        for (int i = 0; i < attributeVector.size(); i++)
        {
            type = attributeVector[i].type;
            typeSize = api->get_type_size(type);
            
            //find the index  of the record, and insert it to index tree
            if (attributeVector[i].index == indexName)
            {
                api->insert_index(indexName, contentBegin, type, block->offsetNum);
                count++;
            }
            
            contentBegin += typeSize;
        }
        recordBegin += recordSize;
    }
    
    return count;
}


bool RecordManager::fit_record_condition(char* recordBegin,int recordSize, vector<Attribute>* attributeVector,vector<Condition>* conditionVector)
{
    if (conditionVector == NULL) {
        return true;
    }
    int type;
    string attributeName;
    int typeSize;
    char content[255];
    
    char *contentBegin = recordBegin;
    for(int i = 0; i < attributeVector->size(); i++)
    {
        type = (*attributeVector)[i].type;
        attributeName = (*attributeVector)[i].name;
        typeSize = api->get_type_size(type);
        
        //init content (when content is string , we can get a string easily)
        memset(content, 0, 255);
        memcpy(content, contentBegin, typeSize);
        for(int j = 0; j < (*conditionVector).size(); j++)
        {
            if ((*conditionVector)[j].attributeName == attributeName)
            {
                if(!fit_content_condition(content, type, &(*conditionVector)[j]))
                {
                    return false;
                }
            }
        }

        contentBegin += typeSize;
    }
    return true;
}


void RecordManager::print_record(char* recordBegin, int recordSize, vector<Attribute>* attributeVector, vector<string> *attributeNameVector)
{
    int type;
    string attributeName;
    int typeSize;
    char content[255];
    
    char *contentBegin = recordBegin;
    for(int i = 0; i < attributeVector->size(); i++)
    {
        type = (*attributeVector)[i].type;
        typeSize = api->get_type_size(type);
        
        //init content (when content is string , we can get a string easily)
        memset(content, 0, 255);
        
        memcpy(content, contentBegin, typeSize);

        for(int j = 0; j < (*attributeNameVector).size(); j++)
        {
            if ((*attributeNameVector)[j] == (*attributeVector)[i].name)
            {
                print_content(content, type);
                break;
            }
        }
        
        contentBegin += typeSize;
    }
}


void RecordManager::print_content(char * content, int type)
{
    if (type == Attribute::TYPE_INT)
    {
        //if the content is a int
        int tmp = *((int *) content);   //get content value by point
        printf("%d ", tmp);
    }
    else if (type == Attribute::TYPE_FLOAT)
    {
        //if the content is a float
        float tmp = *((float *) content);   //get content value by point
        printf("%f ", tmp);
    }
    else
    {
        //if the content is a string
        string tmp = content;
        printf("%s ", tmp.c_str());
    }

}


bool RecordManager::fit_content_condition(char* content,int type,Condition* condition)
{
    if (type == Attribute::TYPE_INT)
    {
        //if the content is a int
        int tmp = *((int *) content);   //get content value by point
        return condition->if_right(tmp);
    }
    else if (type == Attribute::TYPE_FLOAT)
    {
        //if the content is a float
        float tmp = *((float *) content);   //get content value by point
        return condition->if_right(tmp);
    }
    else
    {
        //if the content is a string
        return condition->if_right(content);
    }
    return true;
}


string RecordManager::get_file_name_index(string indexName)
{
    string tmp = "";
    return "INDEX_FILE_"+indexName;
}


string RecordManager::get_file_name_table(string tableName)
{
    string tmp = "";
    return tmp + "TABLE_FILE_" + tableName;
}
