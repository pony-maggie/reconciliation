#!/bin/bash
printf "you want get file '%s\n'" $1
ftp -n<<!
open 61.141.235.66 62110
user agent83916 agent83916@yeahka.com
binary
get $1 $1
close
bye
!

