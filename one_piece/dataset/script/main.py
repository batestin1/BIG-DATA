#!/usr/local/bin/python3
#coding: utf-8
#PERSONA

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: One Piece
#     Repositorio: Json
#     Author: Maycon Cypriano Batestin
#
##################################################################################################################################################################
##################################################################################################################################################################
#imports
from create_data import Variables

print("""
 
   ____  _   _ ______   _____ _____ ______ _____ ______ 
  / __ \| \ | |  ____| |  __ \_   _|  ____/ ____|  ____|
 | |  | |  \| | |__    | |__) || | | |__ | |    | |__   
 | |  | | . ` |  __|   |  ___/ | | |  __|| |    |  __|  
 | |__| | |\  | |____  | |    _| |_| |___| |____| |____ 
  \____/|_| \_|______| |_|   |_____|______\_____|______|
                                                        
                     Expand your maps!
- database of the most sought after by the world government -                                              

""")


val = int(input("INSERT THE AMOUNT OF DATA TO BE GENERATED: "))
Variables(val)
