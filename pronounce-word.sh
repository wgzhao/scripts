#!/bin/bash
TTS_PATH=/usr/share/WyabdcRealPeopleTTS
WORD=`echo $* | tr A-Z a-z`
for WORD2 in $WORD
do
    play ${TTS_PATH}/${WORD2:0:1}/${WORD2}.wav
done
