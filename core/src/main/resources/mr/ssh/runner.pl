#!/usr/bin/perl

$mode = shift @ARGV;
$LINES_PER_CHUNK = 10000;
$MAX_FILES_WAITING = 10;

print STDERR (time())." ";
if ($mode eq "next") {
    my $pid = shift @ARGV;
    my $file = shift @ARGV;
    print STDERR "-> Next $pid $file\n";
    $file =~ /(.*\/)(\d+)$/;
    $index = $2;
    $dir = $1;
    $next = $dir.(++$index);
    if (-e $next) {
        print STDOUT "next $next\n";
        print STDERR "<- $next\n";
    }
    elsif(0 == system("kill -0 $pid 2>/dev/null")) {
        print "alive\n";
        print STDERR "<- alive\n";
    }
    else {
        print "dead\n";
        print STDERR "<- dead\n";
    }
}
elsif ($mode eq "run") {
    $SIG{'HUP'} = "IGNORE";
    my $file = `mktemp`;
    chomp($file);
    $ENV{YT_ERROR_FORMAT}=json;
    `rm -rf $file`;
    `mkdir -p $file`;
    print STDOUT "$$\t$file\n";
    my $bin = shift @ARGV;
    my $command = "$bin '".join("' '", @ARGV)."' 2>&1 |";
    open MR, $command;
    print STDERR "-> Run $$ $file $command\n";
    $index = $LINES_PER_CHUNK;
    my $chunkIndex = -1;
    my $timeStart = time();
    while(<MR>) {
      $index++;
      my $timePassed = time() - $timeStart;
      if ($index > $LINES_PER_CHUNK || $timePassed > 5) {
        $chunkIndex++;
        $index = 0;
        my $nextOut = "$file/$chunkIndex";
        close OUTPUT if $chunkIndex > 0;
        print STDERR "Redirecting to $nextOut\n";
        while (`ls $file | wc -w | awk '{print \$1}'` >= $MAX_FILES_WAITING) {
          sleep 5;
        }
        $timeStart = time();
        open OUTPUT, ">$nextOut";
      }
      print OUTPUT $_;
      $index++;
    }
}
elsif ($mode eq "kill") {
    my $pid = shift @ARGV;
    my $dir = shift @ARGV;
    print STDERR "Kill $pid $file\n";
    `kill -9 $pid 2>/dev/null`;
    `rm -rf $dir`;
}
