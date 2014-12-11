#!/usr/bin/bash


function cleanup() {
  ssh $remote "perl $runner kill $pid $dir 2>>/tmp/runner-errors-$USER.txt";
  gc;
}

trap 'cleanup; exit' SIGINT SIGQUIT SIGHUP

status="alive";
currentFile=$dir/0;
while [ "dead" != "$status" ]; do
  read status file <<< $(ssh $remote "perl $runner next $pid $currentFile 2>>/tmp/runner-errors-$USER.txt")
  if [ "next" == "$status" ]; then
    ssh $remote cat $file;
    currentFile=$file;
  elif [ "alive" == "$status" ]; then
    sleep 5;
  fi
done
ssh $remote rm -rf $dir;

gc;
