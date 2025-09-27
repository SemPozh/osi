#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "command.h"

int main() {
    char input[MAX_CMD_LENGTH];
    while (1) {
        printf("shell> "); 
        if (fgets(input, sizeof(input), stdin) == NULL) {
          printf("fgets returned NULL\n");
          break;
        }
        
        if (strlen(input) <= 1) {
          continue;
        }

        Command* cmd = create_command(input);
        if (cmd) {
            execute_command(cmd);
            free_command(cmd);
        }
    }

    return 0;
}