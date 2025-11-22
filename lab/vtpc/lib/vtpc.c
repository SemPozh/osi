#include "vtpc.h"
#include <fcntl.h>
#include <stddef.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <stdio.h>
#include <sys/stat.h>

#ifndef O_DIRECT
#define O_DIRECT 00040000
#endif

#define LRU_K 2
#define CACHE_BLOCKS 100
#define BLOCK_SIZE 4096
#define MAX_OPEN_FILES 1024

#ifndef MIN
#define MIN(a, b) ((a) < (b) ? (a) : (b))
#endif

typedef struct {
    unsigned long access_times[LRU_K];
    int access_count;
} access_history_t;

typedef struct cache_block {
    int fd;
    off_t block_number;
    char *data;
    int is_dirty;
    access_history_t history;
    struct cache_block *next;
    struct cache_block *prev;
} cache_block_t;

typedef struct {
    cache_block_t *blocks;
    cache_block_t *lru_list;
    int capacity;
    int size;
    unsigned long time_counter;
    int hit_count;
    int miss_count;
} block_cache_t;

typedef struct {
    int real_fd;
    off_t pos;
} file_entry_t;

static block_cache_t *global_cache = NULL;
static file_entry_t open_files[MAX_OPEN_FILES];
static int cache_initialized = 0;

static int init_cache(void);
static cache_block_t* find_block(int fd, off_t block_number);
static cache_block_t* load_block(int fd, off_t block_number);
static void update_access_history(cache_block_t *block);
static cache_block_t* find_victim(void);
static int write_block_to_disk(cache_block_t *block);
static void remove_from_list(cache_block_t *block);
static void add_to_lru_list(cache_block_t *block);
static off_t get_file_size(int fd);

static int init_cache(void) {
    int i, j;
    
    if (global_cache != NULL) {
        return 0;
    }

    global_cache = malloc(sizeof(block_cache_t));
    if (!global_cache) return -1;
    
    global_cache->blocks = calloc(CACHE_BLOCKS, sizeof(cache_block_t));
    if (!global_cache->blocks) {
        free(global_cache);
        return -1;
    }
    
    for (i = 0; i < CACHE_BLOCKS; i++) {
        if (posix_memalign((void**)&global_cache->blocks[i].data, BLOCK_SIZE, BLOCK_SIZE) != 0) {
            for (j = 0; j < i; j++) {
                free(global_cache->blocks[j].data);
            }
            free(global_cache->blocks);
            free(global_cache);
            return -1;
        }
        global_cache->blocks[i].fd = -1;
        global_cache->blocks[i].is_dirty = 0;
        global_cache->blocks[i].history.access_count = 0;
    }
    
    global_cache->lru_list = NULL;
    global_cache->capacity = CACHE_BLOCKS;
    global_cache->size = 0;
    global_cache->time_counter = 0;
    global_cache->hit_count = 0;
    global_cache->miss_count = 0;
    
    for (i = 0; i < MAX_OPEN_FILES; i++) {
        open_files[i].real_fd = -1;
        open_files[i].pos = 0;
    }
    
    cache_initialized = 1;
    return 0;
}

static cache_block_t* find_block(int fd, off_t block_number) {
    int i;
    for (i = 0; i < global_cache->size; i++) {
        cache_block_t *block = &global_cache->blocks[i];
        if (block->fd == fd && block->block_number == block_number) {
            return block;
        }
    }
    return NULL;
}

static void update_access_history(cache_block_t *block) {
    int i;
    global_cache->time_counter++;
    
    for (i = 0; i < LRU_K - 1; i++) {
        block->history.access_times[i] = block->history.access_times[i + 1];
    }
    block->history.access_times[LRU_K - 1] = global_cache->time_counter;
    
    if (block->history.access_count < LRU_K) {
        block->history.access_count++;
    }
    
    remove_from_list(block);
    add_to_lru_list(block);
}

static void remove_from_list(cache_block_t *block) {
    if (block->prev) block->prev->next = block->next;
    if (block->next) block->next->prev = block->prev;
    
    if (global_cache->lru_list == block) {
        global_cache->lru_list = block->next;
    }
    
    block->next = NULL;
    block->prev = NULL;
}

static void add_to_lru_list(cache_block_t *block) {
    block->next = global_cache->lru_list;
    block->prev = NULL;
    
    if (global_cache->lru_list) {
        global_cache->lru_list->prev = block;
    }
    
    global_cache->lru_list = block;
}

static cache_block_t* find_victim(void) {
    cache_block_t *victim = NULL;
    unsigned long oldest_time = 0;
    int i;
    
    for (i = 0; i < global_cache->size; i++) {
        cache_block_t *block = &global_cache->blocks[i];
        unsigned long block_time = 0;
        
        if (block->history.access_count < LRU_K) {
            block_time = 0;
        } else {
            block_time = block->history.access_times[0];
        }
        
        if (!victim || block_time < oldest_time) {
            victim = block;
            oldest_time = block_time;
        }
    }
    
    return victim;
}

static int write_block_to_disk(cache_block_t *block) {
    if (!block || !block->is_dirty) return 0;
    
    ssize_t result = pwrite(block->fd, block->data, BLOCK_SIZE, 
                           block->block_number * BLOCK_SIZE);
    if (result != BLOCK_SIZE) {
        return -1;
    }
    
    block->is_dirty = 0;
    return 0;
}

static off_t get_file_size(int fd) {
    struct stat st;
    if (fstat(fd, &st) < 0) {
        return -1;
    }
    return st.st_size;
}

static cache_block_t* load_block(int fd, off_t block_number) {
    cache_block_t *block = NULL;
    
    if (global_cache->size < global_cache->capacity) {
        block = &global_cache->blocks[global_cache->size];
        global_cache->size++;
    } else {
        block = find_victim();
        if (!block) return NULL;
        
        if (block->is_dirty) {
            if (write_block_to_disk(block) < 0) {
                return NULL;
            }
        }
        
        remove_from_list(block);
    }
    
    off_t file_size = get_file_size(fd);
    off_t block_start = block_number * BLOCK_SIZE;
    
    if (file_size < 0) {
        return NULL;
    }
    
    if (block_start >= file_size) {
        return NULL;
    }
    
    size_t to_read = BLOCK_SIZE;
    if (block_start + BLOCK_SIZE > file_size) {
        to_read = file_size - block_start;
    }
    
    ssize_t result = pread(fd, block->data, to_read, block_start);
    
    if (result < 0) {
        return NULL;
    } else if (result == 0) {
        return NULL;
    } else if (result < BLOCK_SIZE) {
        memset(block->data + result, 0, BLOCK_SIZE - result);
    }
    
    block->fd = fd;
    block->block_number = block_number;
    block->is_dirty = 0;
    block->history.access_count = 0;
    
    update_access_history(block);
    
    return block;
}

int vtpc_open(const char* path, int mode, int access) {
    int i;
    int real_fd;
    
    if (!cache_initialized) {
        if (init_cache() < 0) {
            return -1;
        }
    }
    
    real_fd = open(path, mode | O_DIRECT, access);
    if (real_fd < 0) {
        return -1;
    }
    
    for (i = 0; i < MAX_OPEN_FILES; i++) {
        if (open_files[i].real_fd == -1) {
            open_files[i].real_fd = real_fd;
            open_files[i].pos = 0;
            return i;
        }
    }
    
    close(real_fd);
    return -1;
}

int vtpc_close(int fd) {
    int real_fd;
    int i;
    
    if (fd < 0 || fd >= MAX_OPEN_FILES || open_files[fd].real_fd == -1) {
        return -1;
    }
    
    real_fd = open_files[fd].real_fd;
    
    for (i = 0; i < global_cache->size; i++) {
        cache_block_t *block = &global_cache->blocks[i];
        if (block->fd == real_fd && block->is_dirty) {
            if (write_block_to_disk(block) < 0) {
            }
        }
    }
    
    close(real_fd);
    open_files[fd].real_fd = -1;
    
    return 0;
}

ssize_t vtpc_read(int fd, void* buf, size_t count) {
    file_entry_t *file;
    int real_fd;
    ssize_t total_read;
    char *buffer;
    off_t file_size;
    size_t remaining;
    off_t block_num;
    size_t block_offset;
    size_t to_read;
    cache_block_t *block;
    
    if (fd < 0 || fd >= MAX_OPEN_FILES || open_files[fd].real_fd == -1) {
        errno = EBADF;
        return -1;
    }
    
    file = &open_files[fd];
    real_fd = file->real_fd;
    total_read = 0;
    buffer = buf;
    
    file_size = get_file_size(real_fd);
    if (file_size < 0) {
        return -1;
    }
    
    if (file->pos >= file_size) {
        return 0;
    }
    
    remaining = file_size - file->pos;
    if (count > remaining) {
        count = remaining;
    }
    
    while (count > 0) {
        block_num = file->pos / BLOCK_SIZE;
        block_offset = file->pos % BLOCK_SIZE;
        to_read = count;
        
        if (to_read > BLOCK_SIZE - block_offset) {
            to_read = BLOCK_SIZE - block_offset;
        }
        
        block = find_block(real_fd, block_num);
        
        if (block) {
            global_cache->hit_count++;
            update_access_history(block);
        } else {
            global_cache->miss_count++;
            block = load_block(real_fd, block_num);
            if (!block) {
                if (total_read == 0) {
                    return 0;
                }
                break;
            }
        }
        
        memcpy(buffer + total_read, block->data + block_offset, to_read);
        
        total_read += to_read;
        count -= to_read;
        file->pos += to_read;
    }
    
    return total_read;
}

ssize_t vtpc_write(int fd, const void* buf, size_t count) {
    file_entry_t *file;
    int real_fd;
    ssize_t total_written;
    const char *buffer;
    off_t file_size;
    off_t block_num;
    size_t block_offset;
    size_t to_write;
    cache_block_t *block;
    
    if (fd < 0 || fd >= MAX_OPEN_FILES || open_files[fd].real_fd == -1) {
        errno = EBADF;
        return -1;
    }
    
    file = &open_files[fd];
    real_fd = file->real_fd;
    total_written = 0;
    buffer = buf;
    
    file_size = get_file_size(real_fd);
    if (file_size < 0) {
        return -1;
    }
    
    if (file->pos + count > file_size) {
        if (ftruncate(real_fd, file->pos + count) < 0) {
            return -1;
        }
    }
    
    while (count > 0) {
        block_num = file->pos / BLOCK_SIZE;
        block_offset = file->pos % BLOCK_SIZE;
        to_write = count;
        
        if (to_write > BLOCK_SIZE - block_offset) {
            to_write = BLOCK_SIZE - block_offset;
        }
        
        block = find_block(real_fd, block_num);
        
        if (block) {
            global_cache->hit_count++;
            update_access_history(block);
        } else {
            global_cache->miss_count++;
            block = load_block(real_fd, block_num);
            if (!block) {
                if (total_written > 0) {
                    return total_written;
                }
                return -1;
            }
        }
        
        memcpy(block->data + block_offset, buffer + total_written, to_write);
        block->is_dirty = 1;
        
        total_written += to_write;
        count -= to_write;
        file->pos += to_write;
    }
    
    return total_written;
}

off_t vtpc_lseek(int fd, off_t offset, int whence) {
    file_entry_t *file;
    off_t new_pos;
    off_t file_size;
    
    if (fd < 0 || fd >= MAX_OPEN_FILES || open_files[fd].real_fd == -1) {
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
            file_size = get_file_size(file->real_fd);
            if (file_size < 0) return -1;
            new_pos = file_size + offset;
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
    int real_fd;
    int i;
    
    if (fd < 0 || fd >= MAX_OPEN_FILES || open_files[fd].real_fd == -1) {
        return -1;
    }
    
    real_fd = open_files[fd].real_fd;
    
    for (i = 0; i < global_cache->size; i++) {
        cache_block_t *block = &global_cache->blocks[i];
        if (block->fd == real_fd && block->is_dirty) {
            if (write_block_to_disk(block) < 0) {
                return -1;
            }
        }
    }
    
    return fsync(real_fd);
}

int vtpc_cache_init(void) {
    return init_cache();
}

void vtpc_cache_stats(void) {
    int i;
    
    if (!global_cache) {
        printf("Кэш не инициализирован\n");
        return;
    }
    
    printf("=== Статистика кэша ===\n");
    printf("Размер кэша: %d/%d блоков\n", global_cache->size, global_cache->capacity);
    printf("Попадания: %d\n", global_cache->hit_count);
    printf("Промахи: %d\n", global_cache->miss_count);
    if (global_cache->hit_count + global_cache->miss_count > 0) {
        double hit_ratio = (double)global_cache->hit_count / 
                          (global_cache->hit_count + global_cache->miss_count) * 100;
        printf("Эффективность: %.2f%%\n", hit_ratio);
    }
    
    printf("Блоки в кэше:\n");
    for (i = 0; i < global_cache->size; i++) {
        cache_block_t *block = &global_cache->blocks[i];
        printf("  Блок %d: fd=%d, block=%ld, dirty=%d, accesses=%d\n",
               i, block->fd, (long)block->block_number, 
               block->is_dirty, block->history.access_count);
    }
}