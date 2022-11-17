cmake_minimum_required(VERSION 3.16)
project (hdrdmacp)
set(CMAKE_CXX_STANDARD 20)

#
# Add submodules
#
if(WIN32)
	add_subdirectory(modules/libwinibverbs)
	set(winibverbs_include "modules/libwinibverbs/include")
	set(iverbs_lib winibverbs)
else()
	set(iverbs_lib ibverbs)
endif()

find_package(ZLIB REQUIRED)

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
if(WIN32)
	add_dependencies(libhdrdmacp winibverbs)
endif()
target_compile_definitions(libhdrdmacp PRIVATE _CRT_SECURE_NO_WARNINGS BUILDING_HDRDMA)
target_link_libraries(libhdrdmacp ${iverbs_lib} ${ZLIB_LIBRARY})
target_include_directories(libhdrdmacp PRIVATE ${ZLIB_INCLUDE_DIRS} ${winibverbs_include})

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

if (WIN32)
	add_custom_command(TARGET hdrdmacp POST_BUILD
	  COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_RUNTIME_DLLS:hdrdmacp> $<TARGET_FILE_DIR:hdrdmacp>
	  COMMAND_EXPAND_LISTS
	)
endif()
