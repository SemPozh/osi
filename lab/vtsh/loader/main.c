#define _GNU_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <time.h>
#include <errno.h>

#define BASE_10 10
#define ALIGNMENT 512

int parse_range(const char *range, size_t *left, size_t *right) {
    if (range == NULL) {
        return -1;
    }
    
    int temp_left;
    int temp_right;
    int parsed = sscanf(range, "%d-%d", &temp_left, &temp_right);
    
    if (parsed != 2) {
        return -1;
    }

    if (temp_left < 0 || temp_right < 0 || temp_left > temp_right) {
        return -1;
    }
    
    *left = (size_t)temp_left;
    *right = (size_t)temp_right;
    
    return 0;
}

int main(int argc, char *argv[]) {
    if (argc != 8) {
        printf("You passed %d args, expected 7\n", argc-1);
        printf("Usage: main [rw] [blocks_size] [block_count] [file] [range] [direct] [type]\n");
        return 0;
    }

    char* rw = argv[1];
    size_t block_size = (size_t)strtoull(argv[2], NULL, BASE_10);
    size_t block_count = (size_t)strtoull(argv[3], NULL, BASE_10);
    char* file_path = argv[4];
    size_t left_range;
    size_t right_range;
    bool direct = strcmp(argv[6], "on") == 0;
    char* type = argv[7];

    if (parse_range(argv[5], &left_range, &right_range) != 0) {
        printf("Invalid range format. Use: start-end (with start <= end)\n");
        return -1;
    }

    if (direct && (block_size % ALIGNMENT != 0)) {
        printf("For O_DIRECT, block_size must be multiple of %d\n", ALIGNMENT);
        return -1;
    }

    int fd = -1;
    FILE* file = NULL;
    bool use_direct = direct;

    if (use_direct) {
        fd = open(file_path, O_RDWR | O_CREAT | O_DIRECT, 0644);
        if (fd == -1) {
            perror("Error opening file with O_DIRECT");
            use_direct = false;
        }
    }
    
    if (!use_direct) {
        file = fopen(file_path, "r+b");
        if (file == NULL) {
            file = fopen(file_path, "w+b");
            if (file == NULL) {
                perror("Error opening file for read/write");
                return -1;
            }
        }
    }

    size_t file_size = 0;
    if (use_direct) {
        file_size = lseek(fd, 0, SEEK_END);
        lseek(fd, 0, SEEK_SET);
    } else {
        fseek(file, 0, SEEK_END);
        file_size = ftell(file);
        fseek(file, 0, SEEK_SET);
    }

    size_t start_pos, end_pos;
    bool unlimited_range = false;
    
    if (left_range == 0 && right_range == 0) {
        start_pos = 0;
        if (strcmp(rw, "w") == 0 && strcmp(type, "sequential") == 0) {
            unlimited_range = true;
            end_pos = 0;
        } else {
            end_pos = file_size;
        }
    } else {
        start_pos = left_range;
        end_pos = right_range;
        if (strcmp(rw, "r") == 0 && end_pos > file_size) {
            end_pos = file_size;
        }
    }

    if (!unlimited_range && start_pos > end_pos) {
        printf("Invalid range: start cannot be greater than end\n");
        if (use_direct) close(fd);
        else fclose(file);
        return -1;
    }

    size_t range_size = end_pos - start_pos;
    
    if (!unlimited_range && range_size < block_size && block_count > 0) {
        printf("Range size (%zu) is smaller than block size (%zu)\n", range_size, block_size);
        if (use_direct) close(fd);
        else fclose(file);
        return -1;
    }

    if (strcmp(type, "random") == 0) {
        srand((unsigned int)time(NULL));
    }

    char* buffer = NULL;
    if (use_direct) {
        int result = posix_memalign((void**)&buffer, ALIGNMENT, block_size);
        if (result != 0) {
            printf("Memory allocation failed for O_DIRECT: %s\n", strerror(result));
            if (use_direct) close(fd);
            else if (file) fclose(file);
            return -1;
        }
    } else {
        buffer = malloc(block_size);
        if (buffer == NULL) {
            printf("Memory allocation failed\n");
            if (file) fclose(file);
            return -1;
        }
    }

    printf("Processing: mode=%s, block_size=%zu, block_count=%zu, range=%zu-%zu, type=%s, direct=%s\n",
           rw, block_size, block_count, start_pos, end_pos, type, use_direct ? "on" : "off");

    size_t blocks_processed = 0;
    size_t block_index;
    for (block_index = 0; block_index < block_count; block_index++) {
        size_t current_pos;
        
        if (strcmp(type, "sequential") == 0) {
            current_pos = start_pos + (block_index * block_size);
            if (!unlimited_range && current_pos + block_size > end_pos) {
                break;
            }
        } else if (strcmp(type, "random") == 0) {
            if (range_size < block_size) {
                break;
            }
            size_t max_pos = end_pos - block_size;
            if (start_pos > max_pos) {
                break;
            }
            current_pos = start_pos + (rand() % (max_pos - start_pos + 1));
            current_pos = start_pos + ((current_pos - start_pos) / block_size) * block_size;
        } else {
            printf("Invalid type. Use 'sequential' or 'random'\n");
            break;
        }

        if (strcmp(rw, "r") == 0) {
            if (use_direct) {
                off_t seek_result = lseek(fd, current_pos, SEEK_SET);
                if (seek_result == (off_t)-1) {
                    perror("Error seeking in file");
                    break;
                }
                ssize_t bytes_read = read(fd, buffer, block_size);
                if (bytes_read > 0) {
                    blocks_processed++;
                } else {
                    if (bytes_read == 0) {
                        printf("End of file reached\n");
                    } else {
                        perror("Error reading file");
                    }
                    break;
                }
            } else {
                if (fseek(file, current_pos, SEEK_SET) != 0) {
                    perror("Error seeking in file");
                    break;
                }
                size_t bytes_read = fread(buffer, 1, block_size, file);
                if (bytes_read > 0) {
                    blocks_processed++;
                } else {
                    if (feof(file)) {
                        printf("End of file reached\n");
                    } else {
                        perror("Error reading file");
                    }
                    break;
                }
            }
        } else {
            if (use_direct) {
                if (current_pos + block_size > file_size) {
                    if (ftruncate(fd, current_pos + block_size) == -1) {
                        perror("Error expanding file size");
                        break;
                    }
                    file_size = current_pos + block_size;
                }
                off_t seek_result = lseek(fd, current_pos, SEEK_SET);
                if (seek_result == (off_t)-1) {
                    perror("Error seeking in file");
                    break;
                }
                memset(buffer, 'A' + (block_index % 26), block_size);
                ssize_t bytes_written = write(fd, buffer, block_size);
                if (bytes_written == (ssize_t)block_size) {
                    printf("Written %zd bytes to position %zu\n", bytes_written, current_pos);
                    blocks_processed++;
                } else {
                    perror("Error writing to file");
                    break;
                }
            } else {
                if (current_pos + block_size > file_size) {
                    fseek(file, current_pos + block_size - 1, SEEK_SET);
                    fputc(0, file);
                    fflush(file);
                    file_size = current_pos + block_size;
                }
                if (fseek(file, current_pos, SEEK_SET) != 0) {
                    perror("Error seeking in file");
                    break;
                }
                memset(buffer, 'A' + (block_index % 26), block_size);
                size_t bytes_written = fwrite(buffer, 1, block_size, file);
                if (bytes_written == block_size) {
                    printf("Written %zu bytes to position %zu\n", bytes_written, current_pos);
                    blocks_processed++;
                } else {
                    perror("Error writing to file");
                    break;
                }
            }
        }
    }

    printf("Successfully processed %zu blocks\n", blocks_processed);

    free(buffer);
    
    if (use_direct) {
        if (close(fd) == 0) {
            printf("File closed successfully\n");
        } else {
            perror("Error closing file");
        }
    } else {
        if (file && fclose(file) == 0) {
            printf("File closed successfully\n");
        } else {
            perror("Error closing file");
        }
    }

    return 0;
}