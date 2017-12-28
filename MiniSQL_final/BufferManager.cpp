#include "BufferManager.h"
#include "Minisql.h"
#include <stdlib.h>
#include <string>
#include <cstring>
#include <queue>


BufferManager::BufferManager():total_block(0),total_file(0),fileHead(NULL)
{
    for (int i = 0; i < MAX_FILE_NUM; i ++)
    {
        file_pool[i].fileName = new char[MAX_FILE_NAME];
        if(file_pool[i].fileName == NULL)
        {
            printf("Can not allocate memory in initing the file pool!\n");
            exit (1);
        }
        init_file(file_pool[i]);
    }
    for (int i = 0; i < MAX_BLOCK_NUM; i ++) {
        block_pool[i].address = new char[BLOCK_SIZE];
        if(block_pool[i].address == NULL)
        {
            printf("Can not allocate memory in initing the block pool!\n");
            exit (1);
        }
        block_pool[i].fileName = new char[MAX_FILE_NAME];
        if(block_pool[i].fileName == NULL)
        {
            printf("Can not allocate memory in initing the block pool!\n");
            exit (1);
        }
        init_block(block_pool[i]);
    }
}


BufferManager::~BufferManager()
{
    writtenBackToDiskAll();
    for (int i = 0; i < MAX_FILE_NUM; i ++)
    {
        delete [] file_pool[i].fileName;
    }
    for (int i = 0; i < MAX_BLOCK_NUM; i ++)
    {
        delete [] block_pool[i].address;
    }
}


void BufferManager::init_file(fileNode &file)
{
    file.nextFile = NULL;
    file.preFile = NULL;
    file.blockHead = NULL;
    file.pin = false;
    memset(file.fileName,0,MAX_FILE_NAME);
}


void BufferManager::init_block(blockNode &block)
{
    memset(block.address,0,BLOCK_SIZE);
    size_t init_usage = 0;
    memcpy(block.address, (char*)&init_usage, sizeof(size_t)); // set the block head
    block.using_size = sizeof(size_t);
    block.dirty = false;
    block.nextBlock = NULL;
    block.preBlock = NULL;
    block.offsetNum = -1;
    block.pin = false;
    block.reference = false;
    block.ifbottom = false;
    memset(block.fileName,0,MAX_FILE_NAME);
}


fileNode* BufferManager::getFile(const char * fileName, bool if_pin)
{
    blockNode * btmp = NULL;
    fileNode * ftmp = NULL;
    if(fileHead != NULL)
    {
        for(ftmp = fileHead;ftmp != NULL;ftmp = ftmp->nextFile)
        {
            if(!strcmp(fileName, ftmp->fileName)) //the fileNode is already in the list
            {
                ftmp->pin = if_pin;
                return ftmp;
            }
        }
    }
    // The fileNode is not in the list
    if(total_file == 0) // No file in the list now
    {
        ftmp = &file_pool[total_file];
        total_file ++;
        fileHead = ftmp;
    }
    else if(total_file < MAX_FILE_NUM) // There are empty fileNode in the pool
    {
        ftmp = &file_pool[total_file];
        // add this fileNode into the tail of the list
        file_pool[total_file-1].nextFile = ftmp;
        ftmp->preFile = &file_pool[total_file-1];
        total_file ++;
    }
    else // if total_file >= MAX_FILE_NUM, find one fileNode to replace, write back the block node belonging to the fileNode
    {
        ftmp = fileHead;
        while(ftmp->pin)
        {
            if(ftmp -> nextFile)ftmp = ftmp->nextFile;
            else //no enough file node in the pool
            {
                printf("There are no enough file node in the pool!");
                exit(2);
            }
        }
        for(btmp = ftmp->blockHead;btmp != NULL;btmp = btmp->nextBlock)
        {
            if(btmp->preBlock)
            {
                init_block(*(btmp->preBlock));
                total_block --;
            }
            writtenBackToDisk(btmp->fileName,btmp);
        }
        init_file(*ftmp);
    }
    if(strlen(fileName) + 1 > MAX_FILE_NAME)
    {
        printf("文件名长度过长，最高不能超过%d\n",MAX_FILE_NAME);
        exit(3);
    }
    strncpy(ftmp->fileName, fileName,MAX_FILE_NAME);
    set_pin(*ftmp, if_pin);
    return ftmp;
}


blockNode* BufferManager::get_block(fileNode * file,blockNode *position, bool if_pin)
{
    const char * fileName = file->fileName;
    blockNode * btmp = NULL;
    if(total_block == 0)
    {
        btmp = &block_pool[0];
        total_block ++;
    }
    else if(total_block < MAX_BLOCK_NUM) // there are empty blocks in the block pool
    {
        for(int i = 0 ;i < MAX_BLOCK_NUM;i ++)
        {
            if(block_pool[i].offsetNum == -1)
            {
                btmp = &block_pool[i];
                total_block ++;
                break;
            }
            else
                continue;
        }
    }
    else // total_block >= MAX_BLOCK_NUM,which means that there are no empty block so we must replace one.
    {
        int i = replaced_block;
        while (true)
        {
            i ++;
            if(i >= total_block) i = 0;
            if(!block_pool[i].pin)
            {
                if(block_pool[i].reference == true)
                    block_pool[i].reference = false;
                else //choose this block for replacement
                {
                    btmp = &block_pool[i];
                    if(btmp->nextBlock) btmp -> nextBlock -> preBlock = btmp -> preBlock;
                    if(btmp->preBlock) btmp -> preBlock -> nextBlock = btmp -> nextBlock;
                    if(file->blockHead == btmp) file->blockHead = btmp->nextBlock;
                    replaced_block = i; //record the replaced block and begin from the next block the next time.
                    
                    writtenBackToDisk(btmp->fileName, btmp);
                    init_block(*btmp);
                    break;
                }
            }
            else // this block is been locked
                continue;
        }
    }
    //add the block into the block list
    if(position != NULL && position->nextBlock == NULL)
    {
        btmp -> preBlock = position;
        position -> nextBlock = btmp;
        btmp -> offsetNum = position -> offsetNum + 1;
    }
    else if (position !=NULL && position->nextBlock != NULL)
    {
        btmp->preBlock=position;
        btmp->nextBlock=position->nextBlock;
        position->nextBlock->preBlock=btmp;
        position->nextBlock=btmp;
        btmp -> offsetNum = position -> offsetNum + 1;
    }
    else // the block will be the head of the list
    {
        btmp -> offsetNum = 0;
        if(file->blockHead) // if the file has a wrong block head
        {
            file->blockHead -> preBlock = btmp;
            btmp->nextBlock = file->blockHead;
        }
        file->blockHead = btmp;
    }
    set_pin(*btmp, if_pin);
    if(strlen(fileName) + 1 > MAX_FILE_NAME)
    {
        printf("文件名长度过长，最高不能超过%d\n",MAX_FILE_NAME);
        exit(3);
    }
    strncpy(btmp->fileName, fileName, MAX_FILE_NAME);
    
    //read the file content to the block
    FILE * fileHandle;
    if((fileHandle = fopen(fileName, "ab+")) != NULL)
    {
        if(fseek(fileHandle, btmp->offsetNum*BLOCK_SIZE, 0) == 0)
        {
            if(fread(btmp->address, 1, BLOCK_SIZE, fileHandle)==0)
                btmp->ifbottom = true;
            btmp ->using_size = get_using_size(btmp);
        }
        else
        {
            printf("Problem seeking the file %s in reading",fileName);
            exit(1);
        }
        fclose(fileHandle);
    }
    else
    {
        printf("Problem opening the file %s in reading",fileName);
        exit(1);
    }
    return btmp;
}


void BufferManager::writtenBackToDisk(const char* fileName,blockNode* block)
{
    if(!block->dirty) // this block is not been modified, so it do not need to written back to files
    {
        return;
    }
    else // written back to the file
    {
        FILE * fileHandle = NULL;
        if((fileHandle = fopen(fileName, "rb+")) != NULL)
        {
            if(fseek(fileHandle, block->offsetNum*BLOCK_SIZE, 0) == 0)
            {
                if(fwrite(block->address, block->using_size+sizeof(size_t), 1, fileHandle) != 1)
                {
                    printf("Problem writing the file %s in writtenBackToDisking",fileName);
                    exit(1);
                }
            }
            else
            {
                printf("Problem seeking the file %s in writtenBackToDisking",fileName);
                exit(1);
            }
            fclose(fileHandle);
        }
        else
        {
            printf("Problem opening the file %s in writtenBackToDisking",fileName);
            exit(1);
        }
    }
}


void BufferManager::writtenBackToDiskAll()
{
    blockNode *btmp = NULL;
    fileNode *ftmp = NULL;
    if(fileHead)
    {
        for(ftmp = fileHead;ftmp != NULL;ftmp = ftmp ->nextFile)
        {
            if(ftmp->blockHead)
            {
                for(btmp = ftmp->blockHead;btmp != NULL;btmp = btmp->nextBlock)
                {
                    if(btmp->preBlock)init_block(*(btmp -> preBlock));
                    writtenBackToDisk(btmp->fileName, btmp);
                }
            }
        }
    }
}


blockNode* BufferManager::getNextBlock(fileNode* file,blockNode* block)
{
    if(block->nextBlock == NULL)
    {
        if(block->ifbottom) block->ifbottom = false;
        return get_block(file, block);
    }
    else //block->nextBlock != NULL
    {
        if(block->offsetNum == block->nextBlock->offsetNum - 1)
        {
            return block->nextBlock;
        }
        else //the block list is not in the right order
        {
            return get_block(file, block);
        }
    }
}


blockNode* BufferManager::get_block_head(fileNode* file)
{
    blockNode* btmp = NULL;
    if(file->blockHead != NULL)
    {
        if(file->blockHead->offsetNum == 0) //The right offset of the first block
        {
            btmp = file->blockHead;
        }
        else
        {
            btmp = get_block(file, NULL);
        }
    }
    else// If the file have no block head, get a new block node for it
    {
        btmp = get_block(file,NULL);
    }
    return btmp;
}


blockNode* BufferManager::getBlockByOffset(fileNode* file, int offsetNumber)
{
    blockNode* btmp = NULL;
    if(offsetNumber == 0) return get_block_head(file);
    else
    {
        btmp = get_block_head(file);
        while( offsetNumber > 0)
        {
            btmp = getNextBlock(file, btmp);
            offsetNumber --;
        }
        return btmp;
    }
}


void BufferManager::delete_fileNode(const char * fileName)
{
    fileNode* ftmp = getFile(fileName);
    blockNode* btmp = get_block_head(ftmp);
    queue<blockNode*> blockQ;
    while (true) {
        if(btmp == NULL) return;
        blockQ.push(btmp);
        if(btmp->ifbottom) break;
        btmp = getNextBlock(ftmp,btmp);
    }
    total_block -= blockQ.size();
    while(!blockQ.empty())
    {
        init_block(*blockQ.back());
        blockQ.pop();
    }
    if(ftmp->preFile) ftmp->preFile->nextFile = ftmp->nextFile;
    if(ftmp->nextFile) ftmp->nextFile->preFile = ftmp->preFile;
    if(fileHead == ftmp) fileHead = ftmp->nextFile;
    init_file(*ftmp);
    total_file --;
}


void BufferManager::set_pin(blockNode &block,bool pin)
{
    block.pin = pin;
    if(!pin)
        block.reference = true;
}

void BufferManager::set_pin(fileNode &file,bool pin)
{
    file.pin = pin;
}


void BufferManager::set_dirty(blockNode &block)
{
    block.dirty = true;
}

void BufferManager::clean_dirty(blockNode &block)
{
    block.dirty = false;
}


size_t BufferManager::get_using_size(blockNode* block)
{
    return *(size_t*)block->address;
}

void BufferManager::set_using_size(blockNode & block,size_t usage)
{
    block.using_size = usage;
    memcpy(block.address,(char*)&usage,sizeof(size_t));
}

size_t BufferManager::get_using_size(blockNode & block)
{
    return block.using_size;
}


char* BufferManager::get_content(blockNode& block)
{
    return block.address + sizeof(size_t);
}





