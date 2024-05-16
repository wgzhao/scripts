#!/bin/bash
echo "You pressed: no value yet"
while true; 
do
clear
echo "Bash Extra key Demo, Keys to try"
echo
echo "* Insert Delete, Home, End, Page_Up and Page_Down"
echo "* The four arrow keys"
echo "* Tab, enter, escape, and space key"
echo "* The letter and number keys, etc."
echo
echo "  d = show date/time"
echo "  q = quit"
echo "================================"

case "$key" in
$'\x1b\x5b\x41')  echo "You pressed: up-arrow" ;;
$'\x1b\x5b\x42')  echo "You pressed: down-arrow" ;;
$'\x1b\x5b\x43')  echo "You pressed: right-arrow" ;;
$'\x1b\x5b\x44')  echo "You pressed: left-arrow" ;;
$'\x1b\x5b\x31\x7e')  echo "You pressed: home" ;;
$'\x1b\x5b\x34\x7e')  echo "You pressed: end" ;;
$'\x1b\x5b\x35\x7e')  echo "You pressed: page-up" ;;
$'\x1b\x5b\x36\x7e')  echo "You pressed: page-down" ;;
$'\x1b')  echo "You pressed: escape" ;;
$'\x09')  echo "You pressed: tab" ;;
$'\x0a')  echo "You pressed: enter" ;;
d) date ;;
q) 
echo "Goodbye!"
exit 0
;;
*) echo  "You pressed: $key"
;;
esac

unset K1 K2 K3
read -s -N1 -p "Press a key: "
K1="$REPLY"
read -s -N2 -t 0.001
K2="$REPLY"
read -s -N1 -t 0.001
K3="$REPLY"
key="$K1$K2$K3"
done
exit $?