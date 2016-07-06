#!/bin/bash

cd ./retrievedfiles;
rm -rf *;
cd ..
cd ./storedfiles;
rm -rf *;
cd ..
rm -rf *.db;

