#! /usr/bin/bash
cp ../../../worker/cppworker/worker/SQLRewriter.cpp ./
cp ../../../worker/cppworker/worker/SQLRewriter.h ./
patch SQLRewriter.cpp rewrite.patch
g++ -o validate main.cpp SQLRewriter.cpp
