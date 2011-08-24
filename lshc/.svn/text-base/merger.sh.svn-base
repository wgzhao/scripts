#!/bin/bash
#merger all xml files to a big xml file
usage()
{
	echo "Usage: `basename $0` <stylesheet file> <xml file> <projectname>"
	echo -e "\t style is a .xsl file"	
	echo -e "\t filename should have the .xml suffix"
	echo -e "\t projectname means what's name of the project"
	echo "	`basename $0` newstyle.xsl dailibaoxian.xml"
	exit 65
}
[ $# -gt 2 ] || usage
if [ ! -f "$1" ];then
echo "$1 does not exists"
exit 65
fi
xsl=`basename $1`
sed  -e "s/{projectname}/$3/g" $1 >$xsl
echo "<?xml version='1.0' encoding='utf-8'?>
<?xml-stylesheet type='text/xsl' href='$xsl' ?>
<!-- Post Office Linux System Health Check Report(SHCR) -->
<post>" >/tmp/$$
for i in *.xml
do
cat $i >>/tmp/$$
done
echo "</post>" >>/tmp/$$
mv /tmp/$$ $2
exit 0

