add_library(
    libvtsh
    STATIC
    vtsh.c
    command.c
    builtin.c
)

find_package(
    OpenSSL REQUIRED
)

target_include_directories(
    libvtsh
    PUBLIC
    OpenSSL::SSL 
    OpenSSL::Crypto
    .
)
