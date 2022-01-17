#!/bin/bash

PathToFiles="/public/files"
mkdir -p $PathToFiles

if [ $# -lt 2 ]; then
    echo "USAGE $0 <n_files> <file_size>M"
    exit
fi

n_files=$1
file_size=$2

echo "n_files: $n_files"
echo "file_size: $file_size"

for i in $( seq 1 $n_files )
do 
    filename=${PathToFiles}/file${i}.dat
    echo "Creating $filename with size $file_size"
    dd if=/dev/zero of=$filename bs=$file_size count=1
done