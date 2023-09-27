# deploy
# stage define
# copy file to stage machine

need_deploy_files=$(git diff --name-only $CI_COMMIT_BEFORE_SHA $CI_COMMIT_SHA |grep -v deploy.sh)

for f in ${need_deploy_files[@]}
do
  [ -f $f ] || continue
  echo -n "push $f  "
  meta=$(head -n10 $f |grep  '^#.*location' |tr -d ' '|cut -c 11-)
  if [ "x$meta" = "x" ];then
    echo -e "\t\t[SKIP]"
  else
    host=$(echo $meta |cut -d: -f1 |tr -d ' ')
    path=$(echo $meta |cut -d: -f2 |tr -d ' ')
    user=$(echo $meta |cut -d: -f3 |tr -d ' ')
    scp  ${f} root@${host}:${path}/ >/dev/null 2>&1
    if [ -n "$user" ];then
      ssh root@${host} "chown -R ${user} ${path}/$(basename $f)"
    fi
    if [ $? -eq 0 ];then
      echo -e "\t\t[DONE]"
      exit 0
    else
      echo -e "\t\t[ERROR]"
      exit 1
    fi
  fi
done
