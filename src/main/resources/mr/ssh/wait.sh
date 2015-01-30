#!/usr/bin/bash


function cleanup() {
  ssh -o PasswordAuthentication=no $remote "perl $runner kill $pid $dir 2>>/tmp/runner-errors-$USER.txt";
  gc;
}

trap 'cleanup; exit' SIGINT SIGQUIT SIGHUP

status="alive";
currentFile=$dir/0;
while [ "dead" != "$status" ]; do
  read status file <<< $(ssh -o PasswordAuthentication=no $remote "perl $runner next $pid $currentFile 2>>/tmp/runner-errors-$USER.txt")
#  echo $status 1>&2;
  if [ "next" == "$status" ]; then
    ssh -o PasswordAuthentication=no $remote cat $file;
    currentFile=$file;
  elif [ "alive" == "$status" ]; then
    sleep 1;
  fi
done
#echo exit 1 >&2;

ssh -o PasswordAuthentication=no $remote rm -rf $dir;

#echo exit 2 >&2;

gc;

#echo exit 3 >&2;
