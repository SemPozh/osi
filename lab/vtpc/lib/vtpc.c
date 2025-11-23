#include "vtpc.h"

#include <fcntl.h>
#include <stddef.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <sys/stat.h>
#include <stdio.h>
#include <time.h>

#define MAX_OPEN_FILES 1024
#define VTPC_PATH_MAX 512
#define DEFAULT_LRU_K 2
#define DEFAULT_CACHE_BLOCKS 100
#define DEFAULT_BLOCK_SIZE 4096

/* ==================== Структуры для файловых дескрипторов ==================== */

typedef struct {
    int real_fd;
    off_t pos;
    off_t size; /* tracked logical file size */
    char path[VTPC_PATH_MAX];
    int flags;
    int mode;
    int used;
} file_entry_t;

static file_entry_t open_files[MAX_OPEN_FILES];
static int cache_initialized = 0;
static int g_lru_k = DEFAULT_LRU_K;
static int g_cache_blocks = DEFAULT_CACHE_BLOCKS;

/* ==================== Структуры для LRU-K кэша ==================== */

/* Структура для истории обращений к блоку */
typedef struct access_history {
    long long timestamps[10];  /* До 10 последних обращений (K максимум) */
    int count;                 /* Количество записей в истории */
    int next_index;            /* Индекс для следующей записи (циклический буфер) */
} access_history_t;

/* Структура для блока кэша */
typedef struct cache_block {
    int fd;                    /* Файловый дескриптор */
    off_t block_offset;        /* Смещение блока в файле (выровненное) */
    char* data;                        /* Динамические данные блока */
    int is_dirty;              /* Флаг "грязного" блока (требует записи) */
    access_history_t history;  /* История обращений */
    struct cache_block *prev;  /* Для doubly-linked list */
    struct cache_block *next;  /* Для doubly-linked list */
} cache_block_t;

/* Структура для кэша */
typedef struct {
    cache_block_t **blocks;    /* Массив указателей на блоки */
    cache_block_t *head;       /* Голова LRU списка (самый недавно использованный) */
    cache_block_t *tail;       /* Хвост LRU списка (самый давно использованный) */
    int capacity;              /* Вместимость кэша */
    int size;                  /* Текущий размер кэша */
    int lru_k;                 /* Параметр K для LRU-K */
    long long access_counter;  /* Счетчик обращений для временных меток */
} cache_t;

/* ==================== Глобальные переменные ==================== */

static cache_t *cache = NULL;
static size_t cache_hits = 0;
static size_t cache_misses = 0;

/* ==================== Вспомогательные функции кэша ==================== */

/* Инициализация кэша */
static int init_cache(int capacity, int lru_k) {
    int i;
    
    cache = malloc(sizeof(cache_t));
    if (!cache) return -1;
    
    cache->blocks = calloc(capacity, sizeof(cache_block_t*));
    if (!cache->blocks) {
        free(cache);
        return -1;
    }
    
    cache->capacity = capacity;
    cache->size = 0;
    cache->lru_k = lru_k;
    cache->access_counter = 0;
    cache->head = NULL;
    cache->tail = NULL;
    
    /* Инициализируем все указатели в NULL */
    for (i = 0; i < capacity; i++) {
        cache->blocks[i] = NULL;
    }
    
    return 0;
}

/* Освобождение кэша */
static void free_cache(void) {
    int i;
    
    if (!cache) return;
    
    for (i = 0; i < cache->capacity; i++) {
        if (cache->blocks[i]) {
            free(cache->blocks[i]->data);  /* Освобождаем данные */
            free(cache->blocks[i]);
        }
    }
    
    free(cache->blocks);
    free(cache);
    cache = NULL;
}

/* Хеш-функция для поиска блока в кэше */
static int cache_hash(int fd, off_t offset) {
    off_t block_num = offset / DEFAULT_BLOCK_SIZE;
    return (fd * 31 + block_num) % cache->capacity;
}

/* Обновление истории обращений для блока */
static void update_access_history(cache_block_t *block) {
    access_history_t *history = &block->history;
    
    if (history->count < cache->lru_k) {
        history->timestamps[history->count] = cache->access_counter;
        history->count++;
    } else {
        /* Циклический буфер для K последних обращений */
        history->timestamps[history->next_index] = cache->access_counter;
        history->next_index = (history->next_index + 1) % cache->lru_k;
    }
    
    cache->access_counter++;
}

/* Получение K-го времени доступа (для сравнения в LRU-K) */
static long long get_kth_access_time(const cache_block_t *block) {
    const access_history_t *history = &block->history;
    
    if (history->count < cache->lru_k) {
        /* Если обращений меньше K, возвращаем самое старое */
        return history->timestamps[0];
    } else {
        /* Возвращаем самое старое из K последних */
        return history->timestamps[history->next_index];
    }
}

/* Перемещение блока в голову списка (самый недавно использованный) */
static void move_to_head(cache_block_t *block) {
    if (block == cache->head) return;
    
    /* Удаляем из текущей позиции */
    if (block->prev) block->prev->next = block->next;
    if (block->next) block->next->prev = block->prev;
    
    if (block == cache->tail) {
        cache->tail = block->prev;
    }
    
    /* Добавляем в голову */
    block->prev = NULL;
    block->next = cache->head;
    
    if (cache->head) {
        cache->head->prev = block;
    }
    
    cache->head = block;
    
    if (!cache->tail) {
        cache->tail = block;
    }
}

/* Поиск блока в кэше */
static cache_block_t* find_block(int fd, off_t offset) {
    off_t block_offset = (offset / DEFAULT_BLOCK_SIZE) * DEFAULT_BLOCK_SIZE;
    int hash = cache_hash(fd, block_offset);
    int i;
    
    /* Линейный поиск в bucket'е */
    for (i = 0; i < cache->capacity; i++) {
        int idx = (hash + i) % cache->capacity;
        cache_block_t *block = cache->blocks[idx];
        
        if (block && block->fd == fd && block->block_offset == block_offset) {
            return block;
        }
        
        if (!block) break; /* Конец цепочки */
    }
    
    return NULL;
}

/* Вытеснение блока по алгоритму LRU-K */
static cache_block_t* evict_block(void) {
    cache_block_t *victim = NULL;
    cache_block_t *current = NULL;
    long long oldest_k_time = -1;
    int i;
    
    if (cache->size == 0) return NULL;
    
    /* Ищем блок с самым старым K-м временем доступа */
    current = cache->head;
    while (current) {
        long long k_time = get_kth_access_time(current);
        
        if (oldest_k_time == -1 || k_time < oldest_k_time) {
            oldest_k_time = k_time;
            victim = current;
        }
        
        current = current->next;
    }
    
    if (!victim) {
        /* Fallback: используем хвост списка */
        victim = cache->tail;
    }
    
    /* Удаляем victim из кэша */
    if (victim->is_dirty) {
        /* Синхронизируем грязные данные с диском */
        int real_fd = open_files[victim->fd].real_fd;
        if (real_fd != -1) {
            pwrite(real_fd, victim->data, DEFAULT_BLOCK_SIZE, victim->block_offset);
        }
    }
    
    /* Освобождаем данные блока */
    free(victim->data);
    
    /* Удаляем из хеш-таблицы */
    int hash = cache_hash(victim->fd, victim->block_offset);
    for (i = 0; i < cache->capacity; i++) {
        int idx = (hash + i) % cache->capacity;
        if (cache->blocks[idx] == victim) {
            cache->blocks[idx] = NULL;
            break;
        }
    }
    
    /* Удаляем из linked list */
    if (victim->prev) victim->prev->next = victim->next;
    if (victim->next) victim->next->prev = victim->prev;
    
    if (victim == cache->head) cache->head = victim->next;
    if (victim == cache->tail) cache->tail = victim->prev;
    
    cache->size--;
    
    return victim;
}

/* Добавление блока в кэш */
static cache_block_t* add_block(int fd, off_t offset, const char* data) {
    off_t block_offset = (offset / DEFAULT_BLOCK_SIZE) * DEFAULT_BLOCK_SIZE;
    int i;
    
    /* Если кэш полон, вытесняем блок */
    cache_block_t *block = NULL;
    if (cache->size >= cache->capacity) {
        block = evict_block();
    } else {
        block = malloc(sizeof(cache_block_t));
    }
    
    if (!block) return NULL;
    
    /* Выделяем память для данных блока */
    block->data = malloc(DEFAULT_BLOCK_SIZE);
    if (!block->data) {
        free(block);
        return NULL;
    }
    
    /* Инициализируем блок */
    block->fd = fd;
    block->block_offset = block_offset;
    block->is_dirty = 0;
    memcpy(block->data, data, DEFAULT_BLOCK_SIZE);
    
    /* Инициализируем историю обращений */
    block->history.count = 0;
    block->history.next_index = 0;
    update_access_history(block);
    
    /* Добавляем в хеш-таблицу */
    int hash = cache_hash(fd, block_offset);
    for (i = 0; i < cache->capacity; i++) {
        int idx = (hash + i) % cache->capacity;
        if (!cache->blocks[idx]) {
            cache->blocks[idx] = block;
            break;
        }
    }
    
    /* Добавляем в голову linked list */
    block->prev = NULL;
    block->next = cache->head;
    
    if (cache->head) {
        cache->head->prev = block;
    }
    
    cache->head = block;
    
    if (!cache->tail) {
        cache->tail = block;
    }
    
    cache->size++;
    
    return block;
}

/* Чтение через кэш */
static ssize_t cache_read(int fd, void* buf, size_t count, off_t pos) {
    off_t block_offset = (pos / DEFAULT_BLOCK_SIZE) * DEFAULT_BLOCK_SIZE;
    off_t offset_in_block = pos - block_offset;
    size_t bytes_to_read = count;
    
    if (offset_in_block + count > DEFAULT_BLOCK_SIZE) {
        bytes_to_read = DEFAULT_BLOCK_SIZE - offset_in_block;
    }
    
    /* Ищем блок в кэше */
    cache_block_t *block = find_block(fd, block_offset);
    
    if (block) {
        /* Попадание в кэш */
        cache_hits++;
        update_access_history(block);
        move_to_head(block);
        memcpy(buf, block->data + offset_in_block, bytes_to_read);
        return bytes_to_read;
    } else {
        /* Промах кэша */
        cache_misses++;
        
        /* Читаем с диска */
        int real_fd = open_files[fd].real_fd;
        char block_data[DEFAULT_BLOCK_SIZE];
        ssize_t bytes_read = pread(real_fd, block_data, DEFAULT_BLOCK_SIZE, block_offset);
        
        if (bytes_read <= 0) {
            return bytes_read;
        }
        
        /* Добавляем в кэш */
        block = add_block(fd, block_offset, block_data);
        if (block) {
            update_access_history(block);
        }
        
        /* Копируем запрошенные данные */
        memcpy(buf, block_data + offset_in_block, bytes_to_read);
        return bytes_to_read;
    }
}

/* Запись через кэш */
static ssize_t cache_write(int fd, const void* buf, size_t count, off_t pos) {
    off_t block_offset = (pos / DEFAULT_BLOCK_SIZE) * DEFAULT_BLOCK_SIZE;
    off_t offset_in_block = pos - block_offset;
    size_t bytes_to_write = count;
    
    if (offset_in_block + count > DEFAULT_BLOCK_SIZE) {
        bytes_to_write = DEFAULT_BLOCK_SIZE - offset_in_block;
    }
    
    int real_fd = open_files[fd].real_fd;
    
    /* Ищем блок в кэше */
    cache_block_t *block = find_block(fd, block_offset);
    
    if (block) {
        /* Блок в кэше - обновляем его */
        memcpy(block->data + offset_in_block, buf, bytes_to_write);
        block->is_dirty = 1;
        update_access_history(block);
        move_to_head(block);
        cache_hits++;
    } else {
        /* Блока нет в кэше - создаем новый */
        char block_data[DEFAULT_BLOCK_SIZE];
        
        /* Сначала читаем текущие данные блока */
        ssize_t bytes_read = pread(real_fd, block_data, DEFAULT_BLOCK_SIZE, block_offset);
        if (bytes_read < 0) {
            return -1;
        }
        
        /* Если прочитали меньше чем DEFAULT_BLOCK_SIZE, заполняем нулями */
        if (bytes_read < DEFAULT_BLOCK_SIZE) {
            memset(block_data + bytes_read, 0, DEFAULT_BLOCK_SIZE - bytes_read);
        }
        
        /* Обновляем часть данных */
        memcpy(block_data + offset_in_block, buf, bytes_to_write);
        
        /* Добавляем в кэш */
        block = add_block(fd, block_offset, block_data);
        if (block) {
            block->is_dirty = 1;
            update_access_history(block);
        }
        cache_misses++;
    }
    
    /* Write-through: сразу пишем на диск */
    ssize_t written = pwrite(real_fd, buf, bytes_to_write, pos);
    if (written > 0 && block) {
        /* Если запись успешна, сбрасываем dirty флаг */
        block->is_dirty = 0;
    }
    
    return written;
}

/* ==================== Существующие вспомогательные функции ==================== */

static int init_cache_simple(void) {
    int i;

    if (cache_initialized)
        return 0;

    for (i = 0; i < MAX_OPEN_FILES; ++i) {
        open_files[i].real_fd = -1;
        open_files[i].pos = 0;
        open_files[i].size = 0;
        open_files[i].path[0] = '\0';
        open_files[i].flags = 0;
        open_files[i].mode = 0;
        open_files[i].used = 0;
    }

    /* Инициализируем кэш с текущими параметрами */
    if (init_cache(g_cache_blocks, g_lru_k) < 0) {
        return -1;
    }

    cache_initialized = 1;
    return 0;
}

static void global_inode_refresh(const char *path) {
    int fd;
    if (path == NULL || path[0] == '\0') return;
    fd = open(path, O_RDONLY);
    if (fd >= 0) close(fd);
}

/* Helper: reopen underlying file to invalidate other caches.
   Returns new fd on success, -1 on failure (and leaves original fd unchanged).
*/
static int reopen_real_fd(const char *path, int flags, int mode) {
    int newfd;
    newfd = open(path, flags, mode);
    return newfd;
}

/* Helper to refresh tracked size from real fd (best-effort). */
static void refresh_size_from_fd(int slot) {
    struct stat st;
    int fd;
    if (slot < 0 || slot >= MAX_OPEN_FILES) return;
    if (!open_files[slot].used) return;
    fd = open_files[slot].real_fd;
    if (fd == -1) return;
    if (fstat(fd, &st) == 0) {
        open_files[slot].size = st.st_size;
    }
}

static int sync_and_reopen_slot(int slot) {
    int oldfd;
    int newfd;
    const char *path;
    int flags;
    int mode;
    struct stat st;

    if (slot < 0 || slot >= MAX_OPEN_FILES) return -1;
    if (!open_files[slot].used) return -1;

    oldfd = open_files[slot].real_fd;
    path = open_files[slot].path;
    flags = open_files[slot].flags;
    mode = open_files[slot].mode;

    if (oldfd != -1) {
        /* try to flush */
        (void)fsync(oldfd);
    }

    /* Try to reopen: open new fd first; if success, close old and replace */
    newfd = reopen_real_fd(path, flags, mode);
    if (newfd < 0) {
        /* reopen failed — best-effort: still return -1, but keep old fd and refresh size */
        if (oldfd != -1 && fstat(oldfd, &st) == 0) {
            open_files[slot].size = st.st_size;
        }
        /* try global inode refresh as fallback */
        global_inode_refresh(path);
        return -1;
    }

    /* get size from newfd */
    if (fstat(newfd, &st) == 0) {
        open_files[slot].size = st.st_size;
    }

    /* close old and replace */
    if (oldfd != -1) close(oldfd);
    open_files[slot].real_fd = newfd;
    return 0;
}

/* ==================== API функции ==================== */

int vtpc_open(const char* path, int mode, int access) {
    int real_fd;
    int i;
    int slot;
    struct stat st;

    if (!cache_initialized) {
        if (init_cache_simple() < 0)
            return -1;
    }

    real_fd = open(path, mode, access);
    if (real_fd < 0)
        return -1;

    slot = -1;
    for (i = 0; i < MAX_OPEN_FILES; ++i) {
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

    open_files[slot].real_fd = real_fd;
    open_files[slot].pos = 0;
    open_files[slot].size = 0;
    /* get current size if available */
    if (fstat(real_fd, &st) == 0) {
        open_files[slot].size = st.st_size;
    }

    /* store path and flags/mode for future reopen */
    strncpy(open_files[slot].path, path, VTPC_PATH_MAX - 1);
    open_files[slot].path[VTPC_PATH_MAX - 1] = '\0';
    open_files[slot].flags = mode;
    open_files[slot].mode = access;
    open_files[slot].used = 1;

    return slot;
}

int vtpc_close(int fd) {
    int real_fd;

    if (!cache_initialized)
        init_cache_simple();

    if (fd < 0 || fd >= MAX_OPEN_FILES) {
        errno = EBADF;
        return -1;
    }

    if (!open_files[fd].used) {
        errno = EBADF;
        return -1;
    }

    real_fd = open_files[fd].real_fd;

    /* close underlying */
    if (real_fd != -1) {
        close(real_fd);
    }

    open_files[fd].real_fd = -1;
    open_files[fd].pos = 0;
    open_files[fd].size = 0;
    open_files[fd].path[0] = '\0';
    open_files[fd].flags = 0;
    open_files[fd].mode = 0;
    open_files[fd].used = 0;

    return 0;
}

ssize_t vtpc_read(int fd, void* buf, size_t count) {
    struct stat st;
    off_t filesize;
    off_t remaining;
    ssize_t got;
    int real_fd;

    if (!cache_initialized) {
        if (init_cache_simple() < 0)
            return -1;
    }

    if (fd < 0 || fd >= MAX_OPEN_FILES) {
        errno = EBADF;
        return -1;
    }

    if (!open_files[fd].used) {
        errno = EBADF;
        return -1;
    }

    real_fd = open_files[fd].real_fd;
    if (real_fd == -1) {
        errno = EBADF;
        return -1;
    }

    /* Use tracked size as source of truth; refresh from fd as fallback */
    filesize = open_files[fd].size;
    if (filesize == 0) {
        /* try to get real size */
        if (fstat(real_fd, &st) == 0) {
            filesize = st.st_size;
            open_files[fd].size = filesize;
        }
    }

    if (open_files[fd].pos >= filesize) {
        return 0; /* EOF */
    }

    remaining = filesize - open_files[fd].pos;
    if ((off_t)count > remaining)
        count = (size_t)remaining;

    /* Используем кэш для чтения */
    got = cache_read(fd, buf, count, open_files[fd].pos);
    if (got > 0) {
        open_files[fd].pos += got;
    }

    return got;
}

ssize_t vtpc_write(int fd, const void* buf, size_t count) {
    struct stat st;
    off_t needed_end;
    ssize_t written;
    int real_fd;
    int res;

    if (!cache_initialized)
        init_cache_simple();

    if (fd < 0 || fd >= MAX_OPEN_FILES) {
        errno = EBADF;
        return -1;
    }

    if (!open_files[fd].used) {
        errno = EBADF;
        return -1;
    }

    real_fd = open_files[fd].real_fd;
    if (real_fd == -1) {
        errno = EBADF;
        return -1;
    }

    needed_end = open_files[fd].pos + (off_t)count;

    /* ensure underlying file is large enough */
    if (fstat(real_fd, &st) < 0) {
        return -1;
    }

    if (st.st_size < needed_end) {
        if (ftruncate(real_fd, needed_end) < 0) {
            return -1;
        }
    }

    /* Используем кэш для записи */
    written = cache_write(fd, buf, count, open_files[fd].pos);
    if (written > 0) {
        open_files[fd].pos += written;
        /* update tracked size */
        if (open_files[fd].pos > open_files[fd].size) {
            open_files[fd].size = open_files[fd].pos;
        }
    }

    /* Ensure data is visible to other descriptors/processes */
    res = sync_and_reopen_slot(fd);
    (void)res;

    /* global inode refresh to make size visible to other FDs */
    global_inode_refresh(open_files[fd].path);

    return written;
}

off_t vtpc_lseek(int fd, off_t offset, int whence) {
    struct stat st;
    off_t newpos;
    int real_fd;

    if (!cache_initialized)
        init_cache_simple();

    if (fd < 0 || fd >= MAX_OPEN_FILES) {
        errno = EBADF;
        return -1;
    }

    if (!open_files[fd].used) {
        errno = EBADF;
        return -1;
    }

    real_fd = open_files[fd].real_fd;
    if (real_fd == -1) {
        errno = EBADF;
        return -1;
    }

    if (whence == SEEK_SET) {
        newpos = offset;
    } else if (whence == SEEK_CUR) {
        newpos = open_files[fd].pos + offset;
    } else if (whence == SEEK_END) {
        /* use tracked size as base; refresh from fd if unknown */
        if (open_files[fd].size == 0) {
            if (fstat(real_fd, &st) == 0) {
                open_files[fd].size = st.st_size;
            }
        }
        newpos = open_files[fd].size + offset;
    } else {
        errno = EINVAL;
        return -1;
    }

    if (newpos < 0) {
        errno = EINVAL;
        return -1;
    }

    open_files[fd].pos = newpos;
    return newpos;
}

int vtpc_fsync(int fd) {
    int real_fd;
    int res;

    if (!cache_initialized)
        init_cache_simple();

    if (fd < 0 || fd >= MAX_OPEN_FILES) {
        errno = EBADF;
        return -1;
    }

    if (!open_files[fd].used) {
        errno = EBADF;
        return -1;
    }

    real_fd = open_files[fd].real_fd;
    if (real_fd == -1) {
        errno = EBADF;
        return -1;
    }

    res = fsync(real_fd);
    /* Also try reopen and global inode refresh to ensure other descriptors see updates */
    (void)sync_and_reopen_slot(fd);
    global_inode_refresh(open_files[fd].path);

    return res;
}

/* ==================== Функции управления кэшем ==================== */

int vtpc_cache_init(void) {
    return init_cache_simple();
}

void vtpc_cache_stats(void) {
    size_t total_accesses = cache_hits + cache_misses;
    double hit_ratio = 0.0;
    
    if (total_accesses > 0) {
        hit_ratio = (double)cache_hits / total_accesses * 100.0;
    }
    
    printf("VTPC Cache Statistics:\n");
    printf("  Hits: %zu\n", cache_hits);
    printf("  Misses: %zu\n", cache_misses);
    printf("  Total accesses: %zu\n", total_accesses);
    printf("  Hit ratio: %.2f%%\n", hit_ratio);
    if (cache) {
        printf("  Cache size: %d/%d blocks\n", cache->size, cache->capacity);
        printf("  LRU-K parameter: %d\n", cache->lru_k);
    }
    
    /* Сбрасываем статистику для следующего теста */
    cache_hits = 0;
    cache_misses = 0;
}

void vtpc_set_lru_k(int k) {
    if (k > 0 && k <= 10) {
        g_lru_k = k;
        if (cache) {
            cache->lru_k = k;
        }
    }
}

void vtpc_set_cache_blocks(int blocks) {
    if (blocks > 0 && blocks <= 10000) {
        g_cache_blocks = blocks;
        if (cache) {
            /* Пересоздаем кэш с новым размером */
            free_cache();
            init_cache(blocks, cache->lru_k);
        }
    }
}

size_t vtpc_get_cache_hits(void) {
    return cache_hits;
}

size_t vtpc_get_cache_misses(void) {
    return cache_misses;
}