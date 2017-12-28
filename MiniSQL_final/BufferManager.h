#ifndef __Minisql__BufferManager__
#define __Minisql__BufferManager__
#include "Minisql.h"
#include <stdio.h>

static int replaced_block = -1;


class BufferManager
{
    private:
        fileNode *fileHead;
        fileNode file_pool[MAX_FILE_NUM];
        blockNode block_pool[MAX_BLOCK_NUM];
        int total_block;
        int total_file; 
        void init_block(blockNode & block);
        void init_file(fileNode & file);
        blockNode* get_block(fileNode * file,blockNode* position,bool if_pin = false);
        void writtenBackToDiskAll();
        void writtenBackToDisk(const char* fileName,blockNode* block);
        void clean_dirty(blockNode &block);
        size_t get_using_size(blockNode* block);
        static const int BLOCK_SIZE = 4096;

    public:
        BufferManager();
        ~BufferManager();
        void delete_fileNode(const char * fileName);
        fileNode* getFile(const char* fileName,bool if_pin = false);
        void set_dirty(blockNode & block);
        void set_pin(blockNode & block,bool pin);
        void set_pin(fileNode & file,bool pin);
        void set_using_size(blockNode & block,size_t usage);
        size_t get_using_size(blockNode & block);
        char* get_content(blockNode& block);
        static int get_block_size()     //Get the size of the block
        {
            return BLOCK_SIZE - sizeof(size_t);
        }
        blockNode* getNextBlock(fileNode * file,blockNode* block);
        blockNode* get_block_head(fileNode* file);
        blockNode* getBlockByOffset(fileNode* file, int offestNumber);

};

#endif


