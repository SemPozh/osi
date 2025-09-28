add_executable(
    vtsh
    main.c
)

target_include_directories(
    vtsh
    PUBLIC
    .
)

find_package(
    OpenSSL REQUIRED
)

target_link_libraries(
    vtsh
    PRIVATE
    libvtsh
    OpenSSL::SSL
    OpenSSL::Crypto
)
