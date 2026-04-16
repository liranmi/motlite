cmake_minimum_required(VERSION 3.14)
project(motlite LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_C_STANDARD 11)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE RelWithDebInfo)
endif()

# =====================================================================
# Fetch oro-db (the MOT in-memory engine)
# =====================================================================
include(FetchContent)
FetchContent_Declare(oro-db
    GIT_REPOSITORY https://github.com/liranmi/oro-db.git
    GIT_TAG        main
)
# Only build the core library, not benchmarks/tests
set(ORO_BUILD_BENCH OFF CACHE BOOL "" FORCE)
set(ORO_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(ORO_BUILD_SQLITE OFF CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(oro-db)

# =====================================================================
# Paths
# =====================================================================
set(SQLITE3_SRC_DIR ${CMAKE_SOURCE_DIR}/third_party/sqlite)
set(LINENOISE_DIR   ${CMAKE_SOURCE_DIR}/third_party/linenoise)

# MOT include directories come from the oro target's PUBLIC includes.
# No need to list them here.

# =====================================================================
# SQLite3 library (patched amalgamation)
# =====================================================================
add_library(sqlite3_lib STATIC ${SQLITE3_SRC_DIR}/sqlite3.c)
target_include_directories(sqlite3_lib PUBLIC ${SQLITE3_SRC_DIR})
target_compile_definitions(sqlite3_lib PRIVATE
    SQLITE_THREADSAFE=1
    SQLITE_ENABLE_COLUMN_METADATA=1
)
target_compile_options(sqlite3_lib PRIVATE
    -Wno-unused-parameter
    -Wno-sign-compare
)
target_link_libraries(sqlite3_lib PRIVATE pthread dl m)

# =====================================================================
# MOT adapter (bridges SQLite VDBE ↔ MOT engine)
# =====================================================================
add_library(oro_mot_adapter STATIC
    ${CMAKE_SOURCE_DIR}/sqlite/oro_mot_adapter.cpp
)
target_include_directories(oro_mot_adapter PUBLIC
    ${CMAKE_SOURCE_DIR}/sqlite
    ${SQLITE3_SRC_DIR}
)
target_compile_definitions(oro_mot_adapter PRIVATE
    MOT_STANDALONE=1
    ENABLE_MOT=1
)
target_compile_options(oro_mot_adapter PRIVATE
    -fpermissive -Wno-unused-parameter -Wno-sign-compare
)
target_link_libraries(oro_mot_adapter PUBLIC oro sqlite3_lib)

# sqlite3_lib needs MOT adapter symbols (oroMot* called from sqlite3.c)
target_link_libraries(sqlite3_lib PUBLIC oro_mot_adapter)

# =====================================================================
# Linenoise (line editing for shell)
# =====================================================================
find_library(READLINE_LIB readline)
find_path(READLINE_INCLUDE_DIR readline/readline.h)

add_library(linenoise_lib STATIC
    ${LINENOISE_DIR}/linenoise.c
    ${LINENOISE_DIR}/stringbuf.c
    ${LINENOISE_DIR}/utf8.c
)
target_include_directories(linenoise_lib PRIVATE ${LINENOISE_DIR})
target_compile_options(linenoise_lib PRIVATE -Wno-all)

# =====================================================================
# Shell library (SQLite CLI with renamed main)
# =====================================================================
add_library(sqlite3_shell_lib STATIC ${SQLITE3_SRC_DIR}/shell.c)
target_include_directories(sqlite3_shell_lib PUBLIC ${SQLITE3_SRC_DIR})
target_compile_definitions(sqlite3_shell_lib PRIVATE
    main=sqlite3_shell_main
    SQLITE_ENABLE_COLUMN_METADATA=1
)
if(READLINE_LIB AND READLINE_INCLUDE_DIR)
    message(STATUS "  Line editing: readline")
    target_compile_definitions(sqlite3_shell_lib PRIVATE HAVE_READLINE=1)
else()
    message(STATUS "  Line editing: linenoise (bundled)")
    target_compile_definitions(sqlite3_shell_lib PRIVATE HAVE_READLINE=0 HAVE_LINENOISE=2)
    target_include_directories(sqlite3_shell_lib PRIVATE ${LINENOISE_DIR})
endif()
target_compile_options(sqlite3_shell_lib PRIVATE
    -Wno-unused-parameter -Wno-unused-function -Wno-sign-compare
)
target_link_libraries(sqlite3_shell_lib PUBLIC sqlite3_lib)

# =====================================================================
# oro_shell (interactive CLI)
# =====================================================================
add_executable(oro_shell sqlite/oro_shell.cpp)
target_link_libraries(oro_shell PRIVATE oro_mot_adapter sqlite3_shell_lib)
if(READLINE_LIB AND READLINE_INCLUDE_DIR)
    target_link_libraries(oro_shell PRIVATE readline)
else()
    target_link_libraries(oro_shell PRIVATE linenoise_lib)
endif()
target_include_directories(oro_shell PRIVATE
    ${CMAKE_SOURCE_DIR}/sqlite ${SQLITE3_SRC_DIR})
target_compile_options(oro_shell PRIVATE -Wno-unused-parameter -fpermissive)
add_custom_command(TARGET oro_shell POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_if_different
        ${CMAKE_SOURCE_DIR}/config/mot.conf $<TARGET_FILE_DIR:oro_shell>/mot.conf
)

# =====================================================================
# oro_server (PostgreSQL wire protocol server)
# =====================================================================
add_executable(oro_server sqlite/oro_server.cpp)
target_link_libraries(oro_server PRIVATE oro_mot_adapter sqlite3_lib pthread)
target_include_directories(oro_server PRIVATE
    ${CMAKE_SOURCE_DIR}/sqlite ${SQLITE3_SRC_DIR})
target_compile_options(oro_server PRIVATE -Wno-unused-parameter -fpermissive)
add_custom_command(TARGET oro_server POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_if_different
        ${CMAKE_SOURCE_DIR}/config/mot.conf $<TARGET_FILE_DIR:oro_server>/mot.conf
)

# =====================================================================
# Tests
# =====================================================================
add_executable(motlite_test test/test_mot_engine.cpp)
target_link_libraries(motlite_test PRIVATE oro_mot_adapter sqlite3_lib)
target_include_directories(motlite_test PRIVATE
    ${CMAKE_SOURCE_DIR}/sqlite ${SQLITE3_SRC_DIR})
target_compile_options(motlite_test PRIVATE -Wno-unused-parameter -fpermissive)
add_custom_command(TARGET motlite_test POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_if_different
        ${CMAKE_SOURCE_DIR}/config/mot.conf $<TARGET_FILE_DIR:motlite_test>/mot.conf
)

# =====================================================================
# Summary
# =====================================================================
message(STATUS "")
message(STATUS "=== motlite configuration ===")
message(STATUS "  Build type:   ${CMAKE_BUILD_TYPE}")
message(STATUS "  oro-db from:  ${oro-db_SOURCE_DIR}")
message(STATUS "  Targets:      oro_shell, oro_server, motlite_test")
message(STATUS "")
