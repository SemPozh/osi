#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/wait.h>
#include "command.h"

Command* create_command(char* input) {
    Command* cmd = malloc(sizeof(Command));
    if (!cmd) {
        return NULL;
    }

    cmd->argc = 0;
    
    char* token = strtok(input, " \n");
    while (token != NULL && cmd->argc < MAX_ARGS - 1) {
        cmd->args[cmd->argc++] = token;
        token = strtok(NULL, " \n");
    }
    cmd->args[cmd->argc] = NULL;

    if (cmd->argc == 0) {
        free_command(cmd);
        return NULL;
    }
    
    cmd->name = cmd->args[0];
    return cmd;
}

void free_command(Command* cmd) {
    free(cmd);
}

void execute_external(char* args[]) {
    int status = 0;

    if (args[0] == NULL) {
        printf("Error: No command provided\n");
        return;
    }

    pid_t pid = fork();
    if (pid < 0) {
        perror("fork failed");
        return;
    }

    if (pid == 0) {
        execvp(args[0], args);
        
        perror("exec failed");  
        _exit(EXIT_FAILURE);
    }
    
    if (pid > 0) {
        struct timeval start, end;
        long long start_time, end_time;
    
        gettimeofday(&start, NULL);
        waitpid(pid, &status, 0);
        gettimeofday(&end, NULL);

        start_time = start.tv_sec * 1000000 + start.tv_usec;
        end_time = end.tv_sec * 1000000 + end.tv_usec;
        
        double time_taken_ms = ((double)(end_time - start_time)) / 1000.0;
        printf("Execution time: %.4f ms\n", time_taken_ms);
    }
}

BuiltinCommand builtins[] = {
    {"exit", execute_exit},
    {"mat-mul", execute_mat_mul},
    {"calc-md5", execute_calc_md5},
    {"ema-join-inner", execute_ema_join_inner},
    {"factorize", execute_factorize},
    {NULL, NULL}
};

void execute_command(Command* cmd) {
    if (cmd->argc == 0) {
        return;  
    }

    struct timeval start, end;
    long long start_time, end_time;
    
    gettimeofday(&start, NULL);

    size_t i;

    for (i = 0; builtins[i].name != NULL; i++) {
        if (strcmp(cmd->name, builtins[i].name) == 0) {
            builtins[i].function(cmd->args);
            
            gettimeofday(&end, NULL);
            start_time = start.tv_sec * 1000000 + start.tv_usec;
            end_time = end.tv_sec * 1000000 + end.tv_usec;
            double time_taken_ms = ((double)(end_time - start_time)) / 1000.0;
            printf("Execution time: %.4f ms\n", time_taken_ms);
            return;
        }
    }
    
    execute_external(cmd->args);
}
