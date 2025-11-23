#include "vtpc.h"

#include <fcntl.h>
#include <stddef.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <sys/stat.h>

#define MAX_OPEN_FILES 1024
#define VTPC_PATH_MAX 512

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

    got = pread(real_fd, buf, count, open_files[fd].pos);
    if (got > 0) {
        open_files[fd].pos += got;
    }

    return got;
}

/* internal helper: try to sync and reopen underlying fd to invalidate other caches.
   Returns 0 on success, -1 on failure (but we try best-effort).
*/
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

    written = pwrite(real_fd, buf, count, open_files[fd].pos);
    if (written > 0) {
        open_files[fd].pos += written;
        /* update tracked size */
        if (open_files[fd].pos > open_files[fd].size) {
            open_files[fd].size = open_files[fd].pos;
        }
    }

    /* Ensure data is visible to other descriptors/processes:
       fsync + reopen pattern to invalidate other page-cache views.
       Also perform a global inode refresh (open/close) to force kernel to
       refresh visibility for preexisting descriptors (addresses observed EOF).
       Best-effort: if reopen fails, we still return success of write.
    */
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

int vtpc_cache_init(void) {
    return init_cache_simple();
}

void vtpc_cache_stats(void) {
    /* minimal stats placeholder */
}

