#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
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

    printf("Debug: Trying to execute: '%s'\n", args[0]);
    printf("Debug: Full command line: ");
    int i;
    for (i = 0; args[i] != NULL; i++) {
        printf("'%s' ", args[i]);
    }
    printf("\n");

    pid_t pid = fork();
    if (pid < 0) {
        perror("fork failed");
        return;
    }

    if (pid == 0) {
        execvp(args[0], args);
        
        perror("exec failed");
        printf("Debug: Tried to execute: '%s'\n", args[0]);

        if (access(args[0], F_OK) == -1) {
            printf("Debug: File does not exist: '%s'\n", args[0]);
        } else if (access(args[0], X_OK) == -1) {
            printf("Debug: File exists but is not executable: '%s'\n", args[0]);
        }
        
        _exit(EXIT_FAILURE);
    } else {
        clock_t start_time = clock();
        waitpid(pid, &status, 0);
        clock_t end_time = clock();

        double time_taken_ms = ((double)(end_time - start_time)) / CLOCKS_PER_SEC * SEC_TO_MICROSEC;
        printf("Execution time: %.6f ms\n", time_taken_ms);
        
        if (WIFEXITED(status)) {
            printf("Process exited with status: %d\n", WEXITSTATUS(status));
        } else if (WIFSIGNALED(status)) {
            printf("Process killed by signal: %d\n", WTERMSIG(status));
        }
    }
}

void execute_command(Command* cmd) {
    if (cmd->argc == 0) {
        return;  
    } 
    
    if (strcmp(cmd->name, "exit") == 0) {
        execute_exit(cmd->args);
        return;
    }
    
    if (strcmp(cmd->name, "mat-mul") == 0) {
        execute_mat_mul(cmd->args);
        return;
    }
    
    if (strcmp(cmd->name, "calc-md5") == 0) {
        execute_calc_md5(cmd->args);
        return;
    }
    
    if (strcmp(cmd->name, "ema-join-inner") == 0) {
        execute_ema_join_inner(cmd->args);
        return;
    }

    if (strcmp(cmd->name, "factorize") == 0) {
        execute_factorize(cmd->args);
        return;
    }
    execute_external(cmd->args);
}