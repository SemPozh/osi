add_library(
    libvtsh
    STATIC
    vtsh.c
    command.c
    builtin.c
)

target_include_directories(
    libvtsh
    PUBLIC
    .
)
