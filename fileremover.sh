#!/bin/bash

cd /tmp


cat files2kill.txt |
while read a; 

do
ls -la /$a


done
