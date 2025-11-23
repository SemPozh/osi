add_executable(
    loader
    main.c
)

target_include_directories(
    loader
    PUBLIC
    .
    ../lib
)

target_link_libraries(
    loader
    PRIVATE
    vtpc
)
