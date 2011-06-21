#!/usr/bin/perl -w

## notes
## sqs_util meta <metaobj> wc <worker_count> = sets no:of workers to run
## sqs_util meta <metaobj> qname <name> = creates the queue with name
## sqs_util meta <metaobj> qname <name> tmsg 100  = writes 100 message into the queue
## sqs_util meta <metaobj> wstart = starts no:of workers set in worker count, if wc is not set starts one worker by default
## sqs_util meta <metaobj> wstop = stop all workers currently running
## sqs_util meta <metaobj> wrestart = runs wstop followed by wstart

if(scalar(@ARGV) % 2 != 0){
  push(@ARGV,'cmd');
}

##read arguments into hash
my %hash = @ARGV;

my $rv;
my $domain;
my $qname;

foreach my $cmd ( sort keys %hash )
{
  print "key: $cmd, value: $hash{$cmd}\n";
  if($cmd eq "meta")  
  {
    $rv = system("./aws create-domain $hash{$cmd}");
    if($rv == -1){
      print "creating domain failed: $!\n";
    }else{
      print "list of available domains\n";
      $rv = `./aws list-domains -1`;
      print "$rv";
    }
    $domain = $hash{$cmd};
  }elsif($cmd eq "wc"){
    print "setting worker count\n";
    $rv = system("./aws put-attributes $domain -i meta -n worker_count -v $hash{$cmd} --replace true"); 
    if($rv == -1){
      print "setting worker count failed: $!\n";
    }else{
      $rv = `./aws get-attributes $domain -i meta -n worker_count`;
      print "$rv";
    }
  }elsif($cmd eq "qname"){
    print "setting queue name\n";
    $rv = system("./aws put-attributes $domain -i meta -n queue_name -v $hash{$cmd} --replace true"); 
    if($rv == -1){
      print "setting queue name failed: $!\n";
    }else{
      $rv = `./aws get-attributes $domain -i meta -n queue_name`;
      print "$rv";
    }
    ##creating the queue
    $rv = system("./aws create-queue $hash{$cmd}"); 
    if($rv == -1){
      print "creating queue failed: $!\n";
    }else{
      $rv = `./aws list-queues`;
      print "$rv";
    }
    $qname = $hash{$cmd}; ##set qname
  }elsif($cmd eq "tmsg"){
    #print "getting msg name\n";
    my $tqname = $qname;
    $rv = `./aws get-attributes $domain -i meta -n queue_name -1|awk '{print \$2}'`;
    chomp($rv);
    if($rv eq ""){
      print "getting qname failed\n";
    }else{
      $tqname = $rv;
    }
    ##get queue url
    my $qurl = `./aws list-queues $tqname|grep -o 'https://queue.amazonaws.com[^ ]*'`;
    chomp($qurl);
    $qurl =~ s/https:\/\/queue\.amazonaws\.com//g;
    print "testing [$qurl]\n";
    print "writing test messages..\n";
    my $c = 0;
    for($c = 1; $c <= $hash{$cmd}; $c++){
      print "writing message $c\n";
      my $msgbody = `date +%s.%N`;
      $rv = system("./aws send-message $qurl --message '$c:$msgbody'");
      if($rv == -1){
        print "creating msg failed: $!\n";
      }
    }
    print "writing test messages complete!\n";
  }elsif($cmd eq "wstart"){
    $rv = system("./aws put-attributes $domain -i meta -n worker_run -v 1 --replace true"); 
    if($rv == -1){
      print "meta worker_run update failed: $!\n";
    }
    ##get configured no:of workers and start
    my $wc = 1;
    $rv = `./aws get-attributes $domain -i meta -n worker_count -1|awk '{print \$2}'`;
    chomp($rv);
    if($rv eq ""){
      print "getting worker count failed\n";
    }else{
      $wc = $rv;
    }
    ##start workers
    print "starting $wc workers\n";
    my $w = 1;
    for($w = 1; $w <= $wc ; $w++){
     system("java -classpath 'demo/.:demo/lib/*' SQSWorker > /tmp/sqs.$w.log 2>&1 &");
    }
    print "starting workers complete\n";

  }elsif($cmd eq "wstop"){
    print "stopping workers, it might take a min..\n";
    $rv = system("./aws put-attributes $domain -i meta -n worker_run -v 0 --replace true"); 
    if($rv == -1){
      print "meta worker_run update failed: $!\n";
    }
  }elsif($cmd eq "wrestart"){
    print "stopping workers, it might take a min..\n";
    $rv = system("./aws put-attributes $domain -i meta -n worker_run -v 0 --replace true"); 
    if($rv == -1){
      print "meta worker_run update failed: $!\n";
    }
    sleep(10); #sleep 5secs
    $rv = system("./aws put-attributes $domain -i meta -n worker_run -v 1 --replace true"); 
    if($rv == -1){
      print "meta worker_run update failed: $!\n";
    }
    ##get configured no:of workers and start
     $rv = `./aws get-attributes $domain -i meta -n worker_count -1|awk '{print \$2}'`;
    chomp($rv);
    if($rv eq ""){
      print "getting worker count failed\n";
    }else{
      $wc = $rv;
    }
    ##start workers
    print "starting $wc workers\n";
    my $w = 1;
    for($w = 1; $w <= $wc ; $w++){
     system("java -classpath 'demo/.:demo/lib/*' SQSWorker > /tmp/sqs.$w.log 2>&1 &");
    }
    print "starting workers complete\n";

  }
}







