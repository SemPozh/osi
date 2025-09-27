#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "command.h"


void execute_exit(char** args) {
    printf("Goodbye!\n");
    _exit(0);
}

void execute_mat_mul(char** args) {
    printf("Mat Mul");
}

void execute_calc_md5(char** args) {
    printf("Calc md5");
}

void execute_ema_join_inner(char** args) {
    printf("Ema Join Inner");
}

void execute_factorize(char** args) {
    printf("Factorize");
}