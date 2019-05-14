function packjobs()
{
    url=${1:-"http://211.151.151.246:8081"}
    projname=${2:-"data_center_scheduler"}
    username=${3:-"zhongpeng"}
    passwd=${4:-"zhongpeng1214"}
    cd $HOME/workspace/githome
    #modify cdate variables
    f=$(find azkaban-jobs -type f -name system.properties)
    OS=$(uname -s)
    if [ "$OS" = "Darwin" ];then
        yesday=$(date -v-1d +"%Y-%m-%d")
        /usr/local/bin/gsed -i "/^cdate=/c\cdate=$yesday" $f
    else
        yesday=$(date -d "1 day ago" +"%Y-%m-%d")
        sed -i  "/^cdate=/c\cdate=$yesday" $f
    fi
    cat $f
    rm -f jobs.zip&& find ./azkaban-jobs -type f ! -path '*.git*'  -print |zip jobs.zip -@
    #get session id
    sid=$(curl -s  -k -X POST --data "action=login&username=${username}&password=${passwd}" ${url} |awk -F: '/session.id/ {print  $2}' |tr -d ' "')
    #update new jobs zipped package
    curl -k -i -H "Content-Type: multipart/mixed" -X POST --form "session.id=${sid}" --form 'ajax=upload' --form 'file=@jobs.zip;type=application/zip' --form "project=${projname};type=text/plain"  ${url}/manager
    #execute update_project flow
    curl -k --get --data "session.id=${sid}" --data 'ajax=executeFlow' --data "project=${projname}" --data 'flow=update_project' ${url}/executor
    cd -
}

packjobs
