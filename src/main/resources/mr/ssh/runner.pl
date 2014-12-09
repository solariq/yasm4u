#!/usr/bin/perl

$mode = shift @ARGV;
$LINES_PER_CHUNK = 10000;
$MAX_FILES_WAITING = 10;

print STDERR (time())." ";
if ($mode eq "next") {
    my $pid = shift @ARGV;
    my $file = shift @ARGV;
    print STDERR "Next $pid $file\n";
    $file =~ /(.*\/)(\d+)$/;
    $index = $2;
    $dir = $1;
    `rm -f $file`;
    $next = $dir.(++$index);
    if (-e $next) {
        print STDOUT "next $next\n";
    }
    elsif(0 == system("kill -0 $pid 2>/dev/null")) {
        print "alive\n";
    }
    else {
        print "dead\n";
    }
}
elsif ($mode eq "run") {
    $SIG{'HUP'} = "IGNORE";
    my $file = `mktemp`;
    chomp($file);
    `rm -rf $file`;
    `mkdir -p $file`;
    print STDOUT "$$\t$file\n";
    push @ARGV, "2>&1";

    print STDERR "Run $$ $file ".join(" ", @ARGV)."\n";
    my $bin = shift @ARGV;
    open MR, "$bin '".join("' '", @ARGV)."'|";
    $index = 0;
    while(<MR>) {
      my $modIndex = $index % $LINES_PER_CHUNK;
      my $chunkIndex = 1 + int($index/$LINES_PER_CHUNK);
      if ($modIndex == 0) {
        my $nextOut = "$file/$chunkIndex";
        close OUTPUT if $modIndex > 1;
        print STDERR "Redirecting to $nextOut\n";
        while (`ls $file | wc -w | awk '{print \$1}'` >= $MAX_FILES_WAITING) {
          sleep 5;
        }
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
