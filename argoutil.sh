#!/bin/bash
# create argocd application

function usage()  {
    echo "Usage: $0 -a <application name> -g <group name> [ -n <namespace> ] [ -p <project name> ] [-d <path>]"
    echo "e.g $0 -a redis-ims -g ims -p ims -d redis -s 1"
    exit 1
}

while getopts "a:g:p:d:s:h" option; do
   case $option in
      a) APP=$OPTARG
        ;;
      g) GRP=$OPTARG
        ;;
      p) PROJ=$OPTARG
        ;;
      d) GPATH=$OPTARG
        ;;
      n) NS=$OPTARG
        ;;
      *) # display Help
         usage
         exit;;
   esac
done

if [ -z "$APP" -o -z "$GRP" ];then
    usage
fi

PROJ=${PROJ:-"default"}
GPATH=${GPATH:-$APP}
NS=${NS:-"$GRP"}

argocd app create $APP \
  --grpc-web \
  --repo https://gitlab.lczq.com/grp-arch/k8s-deployment.git \
  --dest-server https://kubernetes.default.svc \
   --path $NS/$GPATH/overlays/test  \
   --dest-namespace $NS \
   --project $PROJ \
   --sync-policy automated