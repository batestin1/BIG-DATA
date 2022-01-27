#! /usr/bin/env bash

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: mars
#     Start Bash 
#     Author: Maycon Cypriano Batestin
#
##################################################################################################################################################################


sleep 1
echo "  ##  ##    #####   #####     ##### ";
sleep 1
echo "  ######       ##   ##  ##   #### ";
sleep 1
echo "  ##  ##   ######   ##         #### ";
sleep 1
echo "  ##  ##    #####   ##       ##### ";
sleep 1
echo "---Welcome to dataset of Perserverence Rovers Mars----"
sleep 1
echo "insert dataset on MongoDB"
sleep 1
mongoimport -d nasa -c mars --type csv --headerline --file dataset.csv
sleep 1
wait 1930
echo "Dataset successfully inserted"