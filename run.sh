#!/bin/bash
go build -o tunnel
if [ $? -eq 0 ]; then
    ./tunnel "$@"
fi