#ifndef COMMAND_H
#define COMMAND_H

#define MAX_CMD_LENGTH 1024
#define MAX_ARGS 10
#define SEC_TO_MICROSEC 1000.0

typedef struct {
    char* name;
    char* args[MAX_ARGS];
    int argc;
} Command;

typedef struct {
    char* name;
    void (*function)(char** args);
} BuiltinCommand;

Command* create_command(char* input);
void execute_command(Command* cmd);
void free_command(Command* cmd);

void execute_exit(char** args);
void execute_mat_mul(char** args);
void execute_calc_md5(char** args);
void execute_ema_join_inner(char** args);
void execute_factorize(char** args);

#endif