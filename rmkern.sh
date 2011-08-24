#!/bin/bash
#remove all unused kernel image packages,as well as headers packages correspendly
sudo aptitude remove $(dpkg -l linux-image* linux-headers* |grep ^ii |grep -v `uname -r` |awk '{print $2}')

