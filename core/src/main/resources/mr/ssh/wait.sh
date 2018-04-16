function cleanup() {
#  echo Running cleanup >>/Users/solar/errors.txt;
  $SSH $remote "perl $runner kill $pid $dir 2>>/tmp/runner-errors-$USER.txt";
  gc;
}

trap 'cleanup; exit' SIGINT SIGQUIT SIGHUP SIGSTOP EXIT

status="alive";
currentFile=$dir/0;

while [ `kill -0 $PPID &>/dev/null; echo $?` == "0" -a "dead" != "$status" ]; do
#  echo Running ssh command [$SSH $remote "perl $runner next $pid $currentFile 2>>/tmp/runner-errors-$USER.txt"] >>/Users/solar/errors.txt;
  read status file <<< $($SSH $remote "perl $runner next $pid $currentFile 2>>/tmp/runner-errors-$USER.txt" 2>/dev/null)
#  echo $status 1>&2 2>>/Users/solar/errors.txt;
  if [ "next" == "$status" ]; then
    $SSH $remote cat $file;
    currentFile=$file;
  elif [ "alive" == "$status" ]; then
    sleep 1;
  fi
done
