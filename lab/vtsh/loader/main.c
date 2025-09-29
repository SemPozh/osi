#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>

#define BASE_10 10

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

    if (temp_left < 0 || temp_right < 0) {
        return -1;
    }
    
    *left = (size_t)temp_left;
    *right = (size_t)temp_right;
    
    return 0;
}

int main(int argc, char *argv[]) {
    if (argc < 7) {
        printf("Usage: main [rw] [blocks_size] [block_count] [file] [range] [direct] [type]\n");
        return 0;
    }
    char* rw;
    size_t block_size;
    size_t block_count;
    char* file;
    size_t left_range;
    size_t right_range;
    bool direct;
    char* type;

    rw = argv[0];
    
    char* endptr;
    block_size = (size_t) strtoull(argv[1], &endptr, BASE_10);
    block_count = (size_t) strtoull(argv[2], &endptr, BASE_10);
    file = argv[3];
    parse_range(argv[4], &left_range, &right_range);
    direct = strcmp(argv[5], "on") == 0;
    type = argv[6];
    return 0;
}