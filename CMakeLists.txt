cmake_minimum_required(VERSION 3.21)
project (hdrdmacp)

#
# Pull submodules.
#
find_package(Git QUIET)
if(GIT_FOUND AND EXISTS "${PROJECT_SOURCE_DIR}/.git")
    option(GIT_SUBMODULE "Check submodules during build" ON)
    if(GIT_SUBMODULE)
        message(STATUS "Submodule update")
        execute_process(COMMAND ${GIT_EXECUTABLE} submodule update --init --recursive
                        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
                        RESULT_VARIABLE GIT_SUBMOD_RESULT)
        if(NOT GIT_SUBMOD_RESULT EQUAL "0")
            message(FATAL_ERROR "git submodule update --init --recursive failed with ${GIT_SUBMOD_RESULT}, please checkout submodules")
        endif()
    endif()
endif()

#
# Add submodules
#
add_subdirectory(modules/libwinibverbs)

#
# Thanks to a bug in CMake, we just manually pass ZLIB_LIBRARY and ZLIB_INCLUDE_DIRS
#
#find_library(ZLIB REQUIRED)
message("ZLIB_LIBRARY: ${ZLIB_LIBRARY}")
message("ZLIB_INCLUDE_DIRS: ${ZLIB_INCLUDE_DIRS}")

add_executable(hdrdmacp
	hdRDMA.cc
	hdRDMA.h
	hdrdmacp.cc
	hdRDMAThread.cc
	hdRDMAThread.h
)
add_dependencies(hdrdmacp winibverbs)
target_compile_definitions(hdrdmacp PRIVATE _CRT_SECURE_NO_WARNINGS)
target_link_libraries(hdrdmacp winibverbs ${ZLIB_LIBRARY})
target_include_directories(hdrdmacp PRIVATE ${ZLIB_INCLUDE_DIRS} modules/libwinibverbs/include)

add_custom_command(TARGET hdrdmacp POST_BUILD
  COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_RUNTIME_DLLS:hdrdmacp> $<TARGET_FILE_DIR:hdrdmacp>
  COMMAND_EXPAND_LISTS
)