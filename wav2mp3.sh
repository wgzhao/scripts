#!/bin/bash

#################################################
#	WHAT IS WOM_audioconverter ?
# Script to
#		convert audio files
#		FROM wav, mp3, ogg, flac or wma
#		TO wav, mp3 and ogg
# WOM audioconverter does not modify the file which you select, it creates a new audio file.
# It cannot convert a directory but you can select several files.
# The bar of progression is useful only if you select more than one file.

#################################################
#		INFO
# Author : yeKcim - yeknan@yahoo.fr - http://yeknan.free.fr
# Licence : GNU GPL
# Dependency
#		zenity
#		mplayer
#		lame
#		vorbis tools
#		flac
# Based on
#		naudilus
#		resize0.1
#		envoiftp
#		http://users.linuxbourg.ch/waver/blog/index.php?2004/11/12/5-convertion-de-wma-en-wav-puis-en-ogg
# History
#		02.11.2005 : v0.3	: better wma support (by Maximiliano Suarez)
#								: spanish translation (by Maximiliano Suarez)
#								: flac support (by Misael Fernandez & yeKcim)
#		24.05.2005 : v0.2	: wma support added
#		05.03.2005 : v0.1	: First public version
# Install
# 		Put on ~/.gnome2/nautilus-scripts/
#		In a console : chmod u+x ~/.gnome2/nautilus-scripts/WOM_audioconverter
version="0.3"
#################################################
#	TRADUCTIONS
	###### Default = English #####
	title="WOM audioconverter "$version""
	pleasesel="Please select at least one file."
	noselec=""$title" convert audio files. "$pleasesel""
	choix="Extension of output file :"
	warning="Warning"
	proceed="is already exist. Overwrite?"
	recur=""$title" can't convert directory. "$pleasesel""
	conversion="Converting file :"
case $LANG in
	######## Spanish ########
	es* )
	title="WOM audioconverter "$version""
	pleasesel="Deberá elegir al menos un archivo a convertir."
	noselec=""$title" permite convertir archivos de audio. "$pleasesel""
	choix="Formato del archivo de salida :"
	warning="Advertencia"
	proceed="el archivo existe. Desea borrar el existente ?"
	recur=""$title" la conversión no es permitida. "$pleasesel""
	conversion="Convirtiendo archivo :";;
	######## Francais ########
	fr* )
	title="WOM audioconverter "$version""
	pleasesel="Merci de selectionner au moins un fichier."
	noselec=""$title" permet de convertir des fichiers audio. "$pleasesel""
	choix="Format du fichier de sortie :"
	warning="Attention"
	proceed="existe deja. Ecraser ?"
	recur=""$title" ne permet pas la conversion de dossiers. "$pleasesel""
	conversion="Conversion du fichier :";;
esac

#################################################
#	FONCTIONS
caf() # fonction "convert audio file"
{
	### Format in_file = mp3 ###
	if [ "`file -b "$1" | grep 'MP3'`" != "" ] || [ "`echo $1 | grep -i '\.mp3$'`" != "" ]
	then
		if [ "$3" = ".ogg" ]
		then # mp3-2-ogg
			lame --quiet --decode "$1" - | oggenc - -Q -b 128 -M 160 -o "$2"
		else # mp3-2-wav
			lame --quiet --decode "$1" "$2"
		fi
		break
	fi
	### Format in_file = ogg ###
	if [ "`file -b "$1" | grep 'Vorbis'`" != "" ] || [ "`echo $1 | grep -i '\.ogg$'`" != "" ]
	then
		if [ "$3" = ".mp3" ]
		then # ogg-2-mp3
			ogg123 -q --device=wav "$1" -f - | lame --quiet -m auto -v -F -b 128 -B 160 -h - "$2"
		else # ogg-2-wav
			ogg123 -q --device=wav "$1" -f "$2"
		fi
		break
	fi
	### Format in_file = wav ###
	if [ "`file -b "$1" | grep 'WAVE'`" != "" ] || [ "`echo $1 | grep -i '\.wav$'`" != "" ]
	then
		if [ "$3" = ".mp3" ]
		then # wav-2-mp3
			lame --quiet -m auto -h -v -F -b 128 -B 160 "$1" "$2"
		else # wav-2-ogg
			oggenc "$1" -Q -b 128 -M 160 -o "$2"
		fi
		break
	fi
	### Format in_file = wma ###
	if [ "`file -b "$1" | grep 'Microsoft'`" != "" ] || [ "`echo $1 | grep -i '\.wma$'`" != "" ]
	then
		if [ "$3" = ".mp3" ]
		then # wma-2-mp3
			mplayer -ao pcm:file="$2.wav" "$1"
			lame --quiet -m auto -h -v -F -b 128 -B 160 "$2.wav" "$2"
			rm -f "$2.wav"
			break
		fi

		if [ "$3" = ".wav" ]
		then # wma-2-wav
			mplayer -ao pcm:file="$2" "$1"
			break
		fi

		if [ "$3" = ".ogg" ]
		then # wma-2-ogg
			mplayer -ao pcm:file="$2.wav" "$1"
			oggenc "$2.wav" -Q -b 128 -M 160 -o "$2"
			rm -f "$2.wav"
			break
		fi
		break
	fi

	### Format in_file = flac ###
	if [ "`file -b "$1" | grep 'Flac'`" != "" ] || [ "`echo $1 | grep -i '\.flac$'`" != "" ]
	then
		ARTIST=`metaflac "$1" --show-tag=ARTIST | sed s/.*=//g`
		TITLE=`metaflac "$1" --show-tag=TITLE | sed s/.*=//g`
		ALBUM=`metaflac "$1" --show-tag=ALBUM | sed s/.*=//g`
		GENRE=`metaflac "$1" --show-tag=GENRE | sed s/.*=//g`
		TRACKNUMBER=`metaflac "$1" --show-tag=TRACKNUMBER | sed s/.*=//g`
		DATE=`metaflac "$1" --show-tag=DATE | sed s/.*=//g`

		if [ "$3" = ".mp3" ]
		then # flac-2-mp3
			flac -c -d "$1" | lame - -m j -b 192 -s 44.1 "$2" 
			id3 -t "$TITLE" -T "$TRACKNUMBER" -y "$DATE" -a "$ARTIST" -A "$ALBUM" -g "$GENRE" "$2"
			break
		fi

		if [ "$3" = ".wav" ]
		then # flac-2-wav
			flac -c -d "$1" -o "$2"
			break
		fi

		if [ "$3" = ".ogg" ]
		then # flac-2-ogg
			flac -c -d "$1" | oggenc - -q6 --artist "$ARTIST" --album "$ALBUM" --tracknum "$TRACKNUMBER" --date "$DATE" --genre "$GENRE" --title "$TITLE" -o "$2"
			break
		fi
	fi

}

#################################################
#	PROGRAMME
#### Pas de fichiers selectionne ###
if [ $# -eq 0 ]; then
	zenity --error --title="$warning" --text="$noselec"
	exit 1
fi
######## Check dependance pour oggenc ou lame #######
depformat=""
if which lame 2>/dev/null
then
	depformat=".mp3"
fi
if which oggenc 2>/dev/null
then
	depformat="$depformat .ogg"
fi		
######## Fenetre principale ########
while [ ! "$formatout" ] # Reafficher la fenetre tant que l'utilisateur n'a pas fait de choix
do
	
			
	formatout=`zenity --title "$title" --list --column="Format" $depformat .wav --text "$choix"`
	###### Choix -> Sortie boucle ######
	if  [ $? != 0 ]; then
		exit 1
	fi
	[ $? -ne 0 ] && exit 2 # Annulation
done
########## Conversion ############
let "nbfiles = $#"
#compteur=0;
(while [ $# -gt 0 ]; do
	for i in $formatout; do
		in_file=$1
		out_file=`echo "$in_file" | sed 's/\.\w*$/'$formatout'/'`		
		echo "# $conversion $in_file"
		i=`echo $i | sed 's/"//g'`
		while `true`; do
			### Le format selectionne est le mÃªme que le format du fichier d'entree ###
			if  [ "$in_file" = "$out_file" ]
			then
				break
			fi
			########## Le fichier de sortie existe deja , l'ecraser ? ##########
			if [ "`ls "$out_file" | grep -v "^ls"`" != "" ]
			then
				if !(`gdialog --title "$warning" --yesno "$out_file $proceed" 200 100`)
				then
					break
				fi
			fi
			caf "$in_file" "$out_file" "$formatout" # Lancer la conversion
		break
		shift
		done
		######### Progression ########
		let "compteur += 1"
		let "progress = compteur*100/nbfiles"
		echo $progress
	done
	shift
done
) |
#### Barre de progression ####
#zenity --progress --title="$title" --auto-close --percentage=0 --pulsate

