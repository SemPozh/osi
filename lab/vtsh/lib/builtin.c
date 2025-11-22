#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <openssl/md5.h>
#include <errno.h>
#include "command.h"


void execute_exit(char** args) {
    printf("Goodbye!\n");
    _exit(0);
}


void execute_mat_mul(char** args) {
    int n;
    int i, j, k;
    int *A, *B, *C;

    if (!args[1]) {
        printf("Usage: mat-mul <size>\n");
        return;
    }

    n = atoi(args[1]);
    if (n <= 0) {
        printf("Matrix size must be positive\n");
        return;
    }

    srand((unsigned)time(NULL));

    A = malloc(n * n * sizeof(int));
    B = malloc(n * n * sizeof(int));
    C = malloc(n * n * sizeof(int));

    if (!A || !B || !C) {
        printf("Memory allocation failed\n");
        free(A);
        free(B);
        free(C);
        return;
    }

    for (i = 0; i < n; i++) {
        for (j = 0; j < n; j++) {
            A[i * n + j] = rand() % 10;
            B[i * n + j] = rand() % 10;
            C[i * n + j] = 0;
        }
    }

    for (i = 0; i < n; i++) {
        for (j = 0; j < n; j++) {
            for (k = 0; k < n; k++) {
                C[i * n + j] += A[i * n + k] * B[k * n + j];
            }
        }
    }

    printf("Matrix A:\n");
    for (i = 0; i < n; i++) {
        for (j = 0; j < n; j++) {
            printf("%3d ", A[i * n + j]);
        }
        printf("\n");
    }

    printf("Matrix B:\n");
    for (i = 0; i < n; i++) {
        for (j = 0; j < n; j++) {
            printf("%3d ", B[i * n + j]);
        }
        printf("\n");
    }

    printf("Matrix C = A * B:\n");
    for (i = 0; i < n; i++) {
        for (j = 0; j < n; j++) {
            printf("%3d ", C[i * n + j]);
        }
        printf("\n");
    }

    free(A);
    free(B);
    free(C);
}


void execute_calc_md5(char** args) {
    int n, i;
    const char* fragments[] = {
        "lorem", "ipsum", "dolor", "sit", "amet",
        "consectetur", "adipiscing", "elit",
        "sed", "do", "eiusmod", "tempor", "incididunt"
    };
    int fragments_count = sizeof(fragments) / sizeof(fragments[0]);
    char buffer[4096];
    unsigned char digest[MD5_DIGEST_LENGTH];

    if (!args[1]) {
        printf("Usage: calc-md5 <count>\n");
        return;
    }

    n = atoi(args[1]);
    if (n <= 0) {
        printf("Count must be positive\n");
        return;
    }

    srand((unsigned)time(NULL));
    buffer[0] = '\0';

    for (i = 0; i < n; i++) {
        const char* frag = fragments[rand() % fragments_count];
        strcat(buffer, frag);
        if (i != n - 1) strcat(buffer, " ");
    }

    MD5((unsigned char*)buffer, strlen(buffer), digest);

    printf("Generated text: %s\n", buffer);
    printf("MD5 hash: ");
    for (i = 0; i < MD5_DIGEST_LENGTH; i++) {
        printf("%02x", digest[i]);
    }
    printf("\n");
}

typedef struct {
    int id;
    char word[9];
} Row;

void execute_ema_join_inner(char** args) {
    int i, j;

    if (!args[1] || !args[2] || !args[3]) {
        fprintf(stderr, "Usage: ema-join-inner <file1> <file2> <output_file>\n");
        return;
    }

    FILE *file1 = fopen(args[1], "r");
    FILE *file2 = fopen(args[2], "r");
    FILE *output = fopen(args[3], "w");
    
    if (!file1 || !file2 || !output) {
        fprintf(stderr, "Error opening files\n");
        if (file1) fclose(file1);
        if (file2) fclose(file2);
        if (output) fclose(output);
        return;
    }

    int n1;
    if (fscanf(file1, "%d", &n1) != 1) {
        fprintf(stderr, "Error reading table1 size\n");
        fclose(file1);
        fclose(file2);
        fclose(output);
        return;
    }

    Row* table1 = malloc(n1 * sizeof(Row));
    for (i = 0; i < n1; i++) {
        char temp_word[100];
        if (fscanf(file1, "%d %99s", &table1[i].id, temp_word) != 2) {
            free(table1);
            fclose(file1);
            fclose(file2);
            fclose(output);
            return;
        }
        if (strlen(temp_word) != 8) {
            fprintf(stderr, "Invalid word length in table1 at row %d: expected 8, got %zu\n", i, strlen(temp_word));
            free(table1);
            fclose(file1);
            fclose(file2);
            fclose(output);
            return;
        }
        strcpy(table1[i].word, temp_word);
    }

    int n2;
    if (fscanf(file2, "%d", &n2) != 1) {
        fprintf(stderr, "Error reading table2 size\n");
        free(table1);
        fclose(file1);
        fclose(file2);
        fclose(output);
        return;
    }

    Row* table2 = malloc(n2 * sizeof(Row));
    for (i = 0; i < n2; i++) {
        char temp_word[100];
        if (fscanf(file2, "%d %99s", &table2[i].id, temp_word) != 2) {
            free(table1);
            free(table2);
            fclose(file1);
            fclose(file2);
            fclose(output);
            return;
        }
        if (strlen(temp_word) != 8) {
            fprintf(stderr, "Invalid word length in table2 at row %d: expected 8, got %zu\n", i, strlen(temp_word));
            free(table1);
            free(table2);
            fclose(file1);
            fclose(file2);
            fclose(output);
            return;
        }
        strcpy(table2[i].word, temp_word);
    }

    int count = 0;
    for (i = 0; i < n1; i++) {
        for (j = 0; j < n2; j++) {
            if (table1[i].id == table2[j].id) {
                count++;
            }
        }
    }

    fprintf(output, "%d\n", count);
    for (i = 0; i < n1; i++) {
        for (j = 0; j < n2; j++) {
            if (table1[i].id == table2[j].id) {
                fprintf(output, "%d %s %s\n", 
                       table1[i].id, 
                       table1[i].word, 
                       table2[j].word);
            }
        }
    }
    
    free(table1);
    free(table2);
    fclose(file1);
    fclose(file2);
    fclose(output);
    printf("Operation completed successfully\n");
}


void execute_factorize(char** args) {
    if (args[1] == NULL) {
        fprintf(stderr, "Usage: factorize <number>\n");
        return;
    }

    char* endptr;
    errno = 0;
    long long num = strtoll(args[1], &endptr, 10);
    
    if (*endptr != '\0' || errno == ERANGE || num < 2) {
        fprintf(stderr, "factorize: invalid number\n");
        return;
    }

    printf("%lld = ", num);
    int first = 1;
    long long divisor = 2;

    while (num > 1) {
        if (num % divisor == 0) {
            if (!first) {
                printf(" * ");
            }
            printf("%lld", divisor);
            num /= divisor;
            first = 0;
        } else {
            divisor++;
        }
    }
    printf("\n");
}