#!/bin/bash

myuid=$(id -u)
mygid=$(id -g)
uidentry=$(getent passwd $myuid)

if [ -z "$uidentry" ] ; then
    # assumes /etc/passwd has root-group (gid 0) ownership
    echo "$myuid:x:$myuid:$mygid:anonymous uid:/tmp:/bin/false" >> /etc/passwd
fi

/bin/bash "$@"
