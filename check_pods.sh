#!/bin/bash
# location: node27:/root/bin
# check pod status
. /etc/profile.d/sms.sh
mobile="15974103570"
day3=$(date -d "3 days ago" +"%s")
namespaces=$(kubectl get ns | grep -Ev 'kube-*|cattle-*|argocd|etcd-backup' | awk '{print $1}' | tail -n+2)
for ns in $namespaces; do
    for pod in $(kubectl get pods -n ${ns} -o name); do
        restartCount=$(kubectl get -n $ns ${pod} -o jsonpath='{ .status.containerStatuses[0].restartCount}')
        if [ $restartCount -gt 0 ];then
            # get exitCode
            exitCode=$(kubectl get -n $ns ${pod} -o jsonpath='{ .status.containerStatuses[0].lastState.terminated.exitCode}')
            # get reason
            reason=$(kubectl get -n $ns ${pod} -o jsonpath='{ .status.containerStatuses[0].lastState.terminated.reason}')
            # get time
            tmpT=$(kubectl get -n $ns ${pod} -o jsonpath='{ .status.containerStatuses[0].lastState.terminated.finishedAt}')
            ts=$(date -d "${tmpT}" +"%s")
            finishedAt=$(date -d "${tmpT}" +"%F %T")
            # it happened in three days
            if [ $ts -ge $day3 ];then
                send_sms $mobile "容器 ${ns}/${pod} 异常重启 ${restartCount} 次，exitCode=${exitCode}, reason=${reason}, time=${finishedAt}"
            fi
        fi
    done
done
