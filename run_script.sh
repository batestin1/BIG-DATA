#! /usr/bin/env bash

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: BIG DATA
#     Start Bash 
#     Author: Maycon Cypriano Batestin
#
##################################################################################################################################################################


echo "  ####    ###    ###          ####     #    #####    # ";
echo "   #  #    #    #   #          #  #   # #     #     # # ";
echo "   #  #    #    #              #  #  #   #    #    #   # ";
echo "   ###     #    #              #  #  #   #    #    #   # ";
echo "   #  #    #    #  ##          #  #  #####    #    ##### ";
echo "   #  #    #    #   #          #  #  #   #    #    #   # ";
echo "  ####    ###    ###          ####   #   #    #    #   # ";
echo "------------ Simulation Projects ------------------------"








echo -choose """which project to start

[1] - Game Of Thrones
[2] - Harry Potter
[3] - Nasa
[4] - One Piece
[5] - Sandman"""
read choose

if [ $choose -eq 1 ]
then
      echo "Welcome to Winter on Game of Throne Project"
      cd IronBankBraavos
      cd dependencies
      pip install -r requeriments.txt
      cd ..
      cd shell
      sh run.sh
elif [ $choose -eq 2 ]
then
      echo "I solemnly swear to do nothing good with Harry Potter Project"
      cd harrypotter
      cd dependencies
      pip install -r requeriments.txt
      cd ..
      sh run_script.sh
elif [ $choose -eq 3 ]
then
      echo "Explore the Universe with Nasa Project"
      cd nasa
      cd dependencies
      pip install -r requeriments.txt
      cd ..
      sh run_script.sh
elif [ $choose -eq 4 ]
then
      echo "Expand Your Maps with One Piece Project"
      cd one_piece
      cd dependencies
      pip install -r requeriments.txt
      cd ..
      sh run_script.sh 
elif [ $choose -eq 5 ]
then
      echo "Dream a litle dream about books with Sandman Project"
      cd sandman
      cd dependencies
      pip install -r requeriments.txt
      cd ..
      sh run.sh
else
      echo "Enter the right answer"
fi

