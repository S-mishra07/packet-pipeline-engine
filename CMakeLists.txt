cmake_minimum_required(VERSION 3.15)
project(PacketPipelineEngine)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(MSVC)
    add_compile_options(/W4)
else()
    add_compile_options(-Wall -Wextra -Wpedantic -O3)
endif()

include_directories(include)

add_executable(pipeline
    src/main.cpp
    src/stages.cpp
)

if(NOT MSVC)
    target_link_libraries(pipeline pthread)
endif()
