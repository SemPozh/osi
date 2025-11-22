#ifndef VTPC_H
#define VTPC_H

#include <sys/types.h>

/* Константы для кэша */
#define LRU_K 2
#define CACHE_BLOCKS 100
#define BLOCK_SIZE 4096

/* API функции */
int vtpc_open(const char* path, int mode, int access);
int vtpc_close(int fd);
ssize_t vtpc_read(int fd, void* buf, size_t count);
ssize_t vtpc_write(int fd, const void* buf, size_t count);
off_t vtpc_lseek(int fd, off_t offset, int whence);
int vtpc_fsync(int fd);

/* Дополнительные функции для управления кэшем */
int vtpc_cache_init(void);
void vtpc_cache_stats(void);

#endif