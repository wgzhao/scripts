#!/bin/bash
# create a token for kubernetes dashbaord

function usage()
{
    echo "usage: $0 <user>  <namespace> [<namespace> ...]"
    exit 0
}

if [ $# -lt 2 ]; then
    usage
    exit 1
fi

user=$1
shift 1

# create sa if not exists
kubectl get serviceaccount sa-${user} -n default 2>/dev/null
if [ $? -eq 0 ]; then
  echo "serviceaccount 'sa-${user}' has exists"
else
  kubectl create serviceaccount sa-${user} -n default
fi
# create cluster role
kubectl get ClusterRole cr-${user}
if [ $? -eq 0 ]; then
  echo "ClusterRole cr-${user} has exists"
else
kubectl create -f - <<EOF
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: cr-${user}
rules:
- apiGroups: [""]
  resources: ["pods","services","deployments", "pods/log","ingresses", "endpoints", "statefulsets", "jobs","cronjobs","daemonsets"]
  verbs: ["get", "watch", "list"]
- apiGroups: [""]
  resources: ["pods/exec"]
  verbs: ["get", "watch", "list","create"]
EOF
fi

rb_file="/tmp/rb.yaml"

>$rb_file

# create rolebinding

for ns in ${@}
do
cat  <<EOF >>$rb_file
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: rb-${user}-${ns}
  namespace: ${ns}
subjects:
- kind: ServiceAccount
  name: sa-${user}
  namespace: default
roleRef:
  kind: ClusterRole
  name: cr-${user}
  apiGroup: rbac.authorization.k8s.io
---
EOF
done

kubectl create -f $rb_file

# get token
secpod=$(kubectl get sa -n default sa-${user} -o jsonpath='{.secrets[0].name}')
token=$(kubectl get secret -n default ${secpod} -o jsonpath='{.data.token}' |base64 -d)
echo "${token}"
