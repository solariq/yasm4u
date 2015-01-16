#!/usr/bin/bash


function cleanup() {
  ssh $remote "perl $runner kill $pid $dir 2>>/tmp/runner-errors-$USER.txt";
  gc;
}

trap 'cleanup; exit' SIGINT SIGQUIT SIGHUP

status="notexists"
typeset -i delay=2
typeset -i tries=0
while [ $status != "exists" ]; do
  read status <<< $(ssh  $remote "if [ -f $runner ] ; then echo 'exists'; else echo 'notexists'; fi")
  sleep delay*tries
  tries=$tries+1
  if [ $tries -ge 5 ]; then
    exit 1
   fi
done

status="alive";
currentFile=$dir/0;
while [ "dead" != "$status" ]; do
  read status file <<< $(ssh $remote "perl $runner next $pid $currentFile 2>>/tmp/runner-errors-$USER.txt")
#  echo $status 1>&2;
  if [ "next" == "$status" ]; then
    ssh $remote cat $file;
    currentFile=$file;
  elif [ "alive" == "$status" ]; then
    sleep 1;
  fi
done
#echo exit 1 >&2;

ssh $remote rm -rf $dir;

#echo exit 2 >&2;

gc;

#echo exit 3 >&2;
