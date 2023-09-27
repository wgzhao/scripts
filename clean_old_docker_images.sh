#!/bin/bash
# clean old image from nexus repoistory
# it needs regctl command, it can be got from https://github.com/regclient/regclient
export PATH=/root/bin:$PATH
registry="nexus.lczq.com"
keep_release_num=7
keep_test_num=3 #include uat and dev

function delete_old_images()
{
    # $1 repo
    # $2 pattern
    # $3 the number of keeping 
    rm -f tag.list
    for tag in $(regctl tag ls "$registry/$1" |grep "$2" )
    do
        # This is the most likely command to fail since the created timestamp is optional, may be set to 0,
        # and the string format might vary.
        # The cut is to remove the "+0000" that breaks the "date" command.
        created=$(regctl image config $registry/$repo:$tag --format '{{.Created}}' | cut -f1,2 -d' ')
        echo "${created} ${tag}" >>tag.list
    done
    # sort by created 
    [ -f tag.list ] || return 
    for tag in $(cat tag.list | sort |head -n-${3} |awk '{print $NF}')
    do
        echo "delete image $registry/$repo:$tag"
        regctl image delete --force-tag-dereference "$registry/$repo:$tag"
    done
}

for repo in $(regctl repo ls "$registry" |grep 'library'); do
    # The "head -n -5" ignores the last 5 tags, but you may want to sort that list first.
    # 1. handle test
    delete_old_images $repo '^test-' $keep_test_num
    # 2. handle uat
    delete_old_images $repo '^uat-' $keep_test_num
    # 3. handle release
    for tag in $(regctl tag ls $registry/$repo |grep -E '^[0-9]{1,}\.[0-9]{1,}\.[0-9]{1,}$' |sort |head -n-${keep_release_num})
    do
        regctl image delete --force-tag-dereference "$registry/$repo:$tag"
    done
done
