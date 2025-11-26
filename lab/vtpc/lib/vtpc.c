#include "vtpc.h"

#include <fcntl.h>
#include <stddef.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <sys/stat.h>
#include <stdint.h>
#include <stdio.h>

#define MAX_OPEN_FILES 1024
#define CACHE_SIZE 256
#define BLOCK_SIZE 4096
#define LRU_K 2

#ifndef O_DIRECT
#define O_DIRECT 040000
#endif

typedef struct cache_block {
    int fd;
    off_t block_no;
    char* data;  /* Изменяем на указатель для выравнивания */
    int dirty;
    unsigned long access_history[LRU_K];
    int access_count;
    struct cache_block* next;
    struct cache_block* prev;
} cache_block_t;

typedef struct {
    cache_block_t* blocks[CACHE_SIZE];
    cache_block_t* lru_head;
    cache_block_t* lru_tail;
    unsigned long access_counter;
} cache_t;

typedef struct {
    int real_fd;
    off_t pos;
    off_t size;
    char path[512];
    int flags;
    int mode;
    int used;
} file_entry_t;

static file_entry_t open_files[MAX_OPEN_FILES];
static cache_t cache;
static int cache_initialized = 0;

static unsigned long hash(int fd, off_t block_no) {
    return ((unsigned long)fd * 2654435761UL + (unsigned long)block_no) % CACHE_SIZE;
}

static void cache_remove_block(cache_block_t* block) {
    if (block->prev) block->prev->next = block->next;
    if (block->next) block->next->prev = block->prev;
    
    if (cache.lru_head == block) cache.lru_head = block->next;
    if (cache.lru_tail == block) cache.lru_tail = block->prev;
    
    unsigned long h = hash(block->fd, block->block_no);
    cache_block_t** bucket = &cache.blocks[h];
    while (*bucket) {
        if (*bucket == block) {
            *bucket = block->next;
            break;
        }
        bucket = &(*bucket)->next;
    }
}

static void cache_add_to_front(cache_block_t* block) {
    block->next = cache.lru_head;
    block->prev = NULL;
    if (cache.lru_head) cache.lru_head->prev = block;
    cache.lru_head = block;
    if (!cache.lru_tail) cache.lru_tail = block;
    
    unsigned long h = hash(block->fd, block->block_no);
    block->next = cache.blocks[h];
    cache.blocks[h] = block;
}

static cache_block_t* cache_find_block(int fd, off_t block_no) {
    unsigned long h = hash(fd, block_no);
    cache_block_t* block = cache.blocks[h];
    int i;
    
    while (block) {
        if (block->fd == fd && block->block_no == block_no) {
            cache.access_counter++;
            
            if (block->access_count < LRU_K) {
                block->access_history[block->access_count++] = cache.access_counter;
            } else {
                for (i = 0; i < LRU_K - 1; i++) {
                    block->access_history[i] = block->access_history[i + 1];
                }
                block->access_history[LRU_K - 1] = cache.access_counter;
            }
            
            cache_remove_block(block);
            cache_add_to_front(block);
            return block;
        }
        block = block->next;
    }
    return NULL;
}

static cache_block_t* find_victim_block(void) {
    cache_block_t* victim = NULL;
    cache_block_t* current = NULL;
    unsigned long min_kth_access = 0;
    unsigned long kth_access = 0;
    int i;
    
    current = cache.lru_head;
    while (current) {
        kth_access = 0;
        if (current->access_count >= LRU_K) {
            kth_access = current->access_history[0];
        }
        
        if (!victim || kth_access < min_kth_access) {
            victim = current;
            min_kth_access = kth_access;
        }
        current = current->next;
    }
    
    return victim;
}

static cache_block_t* cache_allocate_block(int fd, off_t block_no) {
    cache_block_t* new_block = malloc(sizeof(cache_block_t));
    if (!new_block) return NULL;
    
    memset(new_block, 0, sizeof(cache_block_t));
    
    /* Выделяем выровненную память для O_DIRECT */
    if (posix_memalign((void**)&new_block->data, BLOCK_SIZE, BLOCK_SIZE) != 0) {
        free(new_block);
        return NULL;
    }
    
    new_block->fd = fd;
    new_block->block_no = block_no;
    new_block->access_count = 1;
    new_block->access_history[0] = ++cache.access_counter;
    
    cache_add_to_front(new_block);
    return new_block;
}

static int cache_evict_block(void) {
    cache_block_t* victim = find_victim_block();
    if (!victim) return -1;
    
    if (victim->dirty) {
        off_t offset = victim->block_no * BLOCK_SIZE;
        /* Используем O_DIRECT для записи - обходим кеш ОС */
        ssize_t written = pwrite(victim->fd, victim->data, BLOCK_SIZE, offset);
        if (written != BLOCK_SIZE) {
            return -1;
        }
        victim->dirty = 0;
    }
    
    cache_remove_block(victim);
    free(victim->data);  /* Освобождаем выровненную память */
    free(victim);
    return 0;
}

static int init_cache(void) {
    int i;
    
    if (cache_initialized) return 0;
    
    memset(&cache, 0, sizeof(cache_t));
    cache.access_counter = 0;
    
    for (i = 0; i < MAX_OPEN_FILES; i++) {
        open_files[i].real_fd = -1;
        open_files[i].used = 0;
    }
    
    cache_initialized = 1;
    return 0;
}

static int count_used_blocks(void) {
    int count = 0;
    int i;
    cache_block_t* block;
    
    for (i = 0; i < CACHE_SIZE; i++) {
        block = cache.blocks[i];
        while (block) {
            count++;
            block = block->next;
        }
    }
    return count;
}

static int ensure_cache_space(void) {
    int used_blocks = count_used_blocks();
    if (used_blocks < CACHE_SIZE) return 0;
    
    return cache_evict_block();
}

static int read_into_cache(int fd, off_t block_no) {
    cache_block_t* block;
    off_t offset;
    ssize_t read_bytes;
    
    if (ensure_cache_space() < 0) return -1;
    
    block = cache_allocate_block(fd, block_no);
    if (!block) return -1;
    
    offset = block_no * BLOCK_SIZE;
    /* Используем O_DIRECT для чтения - обходим кеш ОС */
    read_bytes = pread(fd, block->data, BLOCK_SIZE, offset);
    if (read_bytes < 0) {
        cache_remove_block(block);
        free(block->data);
        free(block);
        return -1;
    }
    
    if (read_bytes < BLOCK_SIZE) {
        memset(block->data + read_bytes, 0, BLOCK_SIZE - read_bytes);
    }
    
    return 0;
}

static int flush_block(cache_block_t* block) {
    off_t offset;
    ssize_t written;
    
    if (!block->dirty) return 0;
    
    offset = block->block_no * BLOCK_SIZE;
    /* Используем O_DIRECT для записи - обходим кеш ОС */
    written = pwrite(block->fd, block->data, BLOCK_SIZE, offset);
    if (written != BLOCK_SIZE) return -1;
    
    block->dirty = 0;
    return 0;
}

static int flush_file_blocks(int fd) {
    int i;
    cache_block_t* block;
    
    for (i = 0; i < CACHE_SIZE; i++) {
        block = cache.blocks[i];
        while (block) {
            if (block->fd == fd && block->dirty) {
                if (flush_block(block) < 0) return -1;
            }
            block = block->next;
        }
    }
    return 0;
}

static void remove_file_blocks(int fd) {
    int i;
    cache_block_t** bucket;
    cache_block_t* to_remove;
    
    for (i = 0; i < CACHE_SIZE; i++) {
        bucket = &cache.blocks[i];
        while (*bucket) {
            if ((*bucket)->fd == fd) {
                to_remove = *bucket;
                *bucket = to_remove->next;
                
                if (to_remove->prev) to_remove->prev->next = to_remove->next;
                if (to_remove->next) to_remove->next->prev = to_remove->prev;
                
                if (cache.lru_head == to_remove) cache.lru_head = to_remove->next;
                if (cache.lru_tail == to_remove) cache.lru_tail = to_remove->prev;
                
                free(to_remove->data);
                free(to_remove);
            } else {
                bucket = &(*bucket)->next;
            }
        }
    }
}

int vtpc_open(const char* path, int mode, int access) {
    int real_fd;
    int slot;
    int i;
    struct stat st;
    
    if (!cache_initialized) init_cache();
    
    /* Открываем файл с O_DIRECT для обхода кеша ОС */
    real_fd = open(path, mode | O_DIRECT, access);
    if (real_fd < 0) {
        /* Если O_DIRECT не поддерживается, пробуем без него */
        real_fd = open(path, mode, access);
        if (real_fd < 0) return -1;
    }
    
    slot = -1;
    for (i = 0; i < MAX_OPEN_FILES; i++) {
        if (!open_files[i].used) {
            slot = i;
            break;
        }
    }
    
    if (slot == -1) {
        close(real_fd);
        errno = EMFILE;
        return -1;
    }
    
    if (fstat(real_fd, &st) < 0) {
        close(real_fd);
        return -1;
    }
    
    open_files[slot].real_fd = real_fd;
    open_files[slot].pos = 0;
    open_files[slot].size = st.st_size;
    strncpy(open_files[slot].path, path, sizeof(open_files[slot].path) - 1);
    open_files[slot].path[sizeof(open_files[slot].path) - 1] = '\0';
    open_files[slot].flags = mode;
    open_files[slot].mode = access;
    open_files[slot].used = 1;
    
    return slot;
}

int vtpc_close(int fd) {
    int result;
    
    if (fd < 0 || fd >= MAX_OPEN_FILES || !open_files[fd].used) {
        errno = EBADF;
        return -1;
    }
    
    if (flush_file_blocks(open_files[fd].real_fd) < 0) {
        return -1;
    }
    
    remove_file_blocks(open_files[fd].real_fd);
    
    result = close(open_files[fd].real_fd);
    
    open_files[fd].real_fd = -1;
    open_files[fd].used = 0;
    open_files[fd].path[0] = '\0';
    
    return result;
}

ssize_t vtpc_read(int fd, void* buf, size_t count) {
    file_entry_t* file;
    char* buffer;
    size_t total_read;
    cache_block_t* block;
    off_t block_no;
    size_t block_offset;
    size_t to_read;
    
    if (fd < 0 || fd >= MAX_OPEN_FILES || !open_files[fd].used) {
        errno = EBADF;
        return -1;
    }
    
    file = &open_files[fd];
    
    if (file->pos >= file->size) return 0;
    
    if (file->pos + count > file->size) {
        count = file->size - file->pos;
    }
    
    buffer = (char*)buf;
    total_read = 0;
    
    while (count > 0) {
        block_no = file->pos / BLOCK_SIZE;
        block_offset = file->pos % BLOCK_SIZE;
        to_read = BLOCK_SIZE - block_offset;
        if (to_read > count) to_read = count;
        
        block = cache_find_block(file->real_fd, block_no);
        if (!block) {
            if (read_into_cache(file->real_fd, block_no) < 0) {
                return -1;
            }
            block = cache_find_block(file->real_fd, block_no);
            if (!block) return -1;
        }
        
        memcpy(buffer, block->data + block_offset, to_read);
        
        buffer += to_read;
        file->pos += to_read;
        total_read += to_read;
        count -= to_read;
    }
    
    return total_read;
}

ssize_t vtpc_write(int fd, const void* buf, size_t count) {
    file_entry_t* file;
    const char* buffer;
    size_t total_written;
    cache_block_t* block;
    off_t block_no;
    size_t block_offset;
    size_t to_write;
    off_t offset;
    ssize_t read_bytes;
    
    if (fd < 0 || fd >= MAX_OPEN_FILES || !open_files[fd].used) {
        errno = EBADF;
        return -1;
    }
    
    file = &open_files[fd];
    buffer = (const char*)buf;
    total_written = 0;
    
    while (count > 0) {
        block_no = file->pos / BLOCK_SIZE;
        block_offset = file->pos % BLOCK_SIZE;
        to_write = BLOCK_SIZE - block_offset;
        if (to_write > count) to_write = count;
        
        block = cache_find_block(file->real_fd, block_no);
        if (!block) {
            if (ensure_cache_space() < 0) return -1;
            block = cache_allocate_block(file->real_fd, block_no);
            if (!block) return -1;
            
            if (block_offset > 0 || to_write < BLOCK_SIZE) {
                offset = block_no * BLOCK_SIZE;
                /* Используем O_DIRECT для чтения - обходим кеш ОС */
                read_bytes = pread(file->real_fd, block->data, BLOCK_SIZE, offset);
                if (read_bytes < 0 && errno != EINTR) {
                    cache_remove_block(block);
                    free(block->data);
                    free(block);
                    return -1;
                }
            }
        }
        
        memcpy(block->data + block_offset, buffer, to_write);
        block->dirty = 1;
        
        buffer += to_write;
        file->pos += to_write;
        total_written += to_write;
        count -= to_write;
        
        if (file->pos > file->size) {
            file->size = file->pos;
        }
    }
    
    return total_written;
}

off_t vtpc_lseek(int fd, off_t offset, int whence) {
    file_entry_t* file;
    off_t new_pos;
    
    if (fd < 0 || fd >= MAX_OPEN_FILES || !open_files[fd].used) {
        errno = EBADF;
        return -1;
    }
    
    file = &open_files[fd];
    
    switch (whence) {
        case SEEK_SET:
            new_pos = offset;
            break;
        case SEEK_CUR:
            new_pos = file->pos + offset;
            break;
        case SEEK_END:
            new_pos = file->size + offset;
            break;
        default:
            errno = EINVAL;
            return -1;
    }
    
    if (new_pos < 0) {
        errno = EINVAL;
        return -1;
    }
    
    file->pos = new_pos;
    return new_pos;
}

int vtpc_fsync(int fd) {
    if (fd < 0 || fd >= MAX_OPEN_FILES || !open_files[fd].used) {
        errno = EBADF;
        return -1;
    }
    
    if (flush_file_blocks(open_files[fd].real_fd) < 0) {
        return -1;
    }
    
    return fsync(open_files[fd].real_fd);
}

int vtpc_cache_init(void) {
    return init_cache();
}

void vtpc_cache_stats(void) {
    int used_blocks = count_used_blocks();
    printf("Cache stats: %d/%d blocks used\n", used_blocks, CACHE_SIZE);
}