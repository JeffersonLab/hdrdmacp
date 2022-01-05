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

#===============================================================================
# LIBRARY
#===============================================================================
add_library(libhdrdmacp SHARED
	IhdRDMA.h
	hdRDMA.cc
	hdRDMA.h
	hdRDMAThread.cc
	hdRDMAThread.h
)
add_dependencies(libhdrdmacp winibverbs)
target_compile_definitions(libhdrdmacp PRIVATE _CRT_SECURE_NO_WARNINGS BUILDING_HDRDMA)
target_link_libraries(libhdrdmacp winibverbs ${ZLIB_LIBRARY})
target_include_directories(libhdrdmacp PRIVATE ${ZLIB_INCLUDE_DIRS} modules/libwinibverbs/include)

set_target_properties(libhdrdmacp PROPERTIES OUTPUT_NAME hdrdmacp)

install(TARGETS libhdrdmacp DESTINATION ${CMAKE_INSTALL_PREFIX})
install(FILES 
	IhdRDMA.h
	DESTINATION include)

#===============================================================================
# APPLICATION
#===============================================================================
add_executable(hdrdmacp
	hdrdmacp.cc
)
add_dependencies(hdrdmacp libhdrdmacp)
target_link_libraries(hdrdmacp libhdrdmacp)

add_custom_command(TARGET hdrdmacp POST_BUILD
  COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_RUNTIME_DLLS:hdrdmacp> $<TARGET_FILE_DIR:hdrdmacp>
  COMMAND_EXPAND_LISTS
)