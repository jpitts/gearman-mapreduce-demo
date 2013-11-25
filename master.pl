#!/usr/bin/env perl

=head1 NAME

Master Process

=head1 SYNOPSIS

./master.pl

=head1 DESCRIPTION

The Master controls the mapping and reducing process. First, gearman clients for Mapper and Reducer jobs are created. Then the corpus of company filings are divided into splits. Each split is submitted for mapping. The jobs are monitored inside of a while loop. As Mapper jobs come to completion, Master submits Reducer jobs. When all Mapper and Reducer jobs are completed, the monitor loop exits. 

=cut

use strict;
use warnings;

use Gearman::XS qw(:constants);
use Gearman::XS::Client;
use FreezeThaw qw(freeze thaw cmpStr safeFreeze cmpStrHard);

# set up the mapper
print "Connect to the Mapper gearman servers.\n";
my $mapper = new Gearman::XS::Client;
if ($mapper->add_server('localhost', 4730) != GEARMAN_SUCCESS) {
	printf(STDERR "%s\n", $mapper->error());
	exit(1);
}

# set up the reducer, generate a sortable unique id
print "Connect to the Reducer gearman servers.\n";
my $reducer = new Gearman::XS::Client;
if ($reducer->add_server('localhost', 4731) != GEARMAN_SUCCESS) {
	printf(STDERR "%s\n", $reducer->error());
	exit(1);
}
my $reducer_id = time . '_' . join "", map { ("a".."z", 0..9)[rand 36] } (1..4);


# create splits (in this case two companies)
my @splits = (qw/ 320193 1288776 /);
print "Split the corpus into " . $#splits . "parts.\n";


print "Begin mapping and reducing.\n";

# submit jobs with each split to the mappers
my ($ret, $job_handle);
my $jobs = {};
foreach my $split (@splits) {
	
	# submit a mapper job to be performed by gearman workers
	($ret, $job_handle) = $mapper->do_background( 'mapper',	
		freeze ({ # workload
			'split' => int($split) 
		})
	);
	
	# add this to the jobs to be monitored
	if ($ret == GEARMAN_SUCCESS) {
		print "> Begin mapping $split with job_handle=$job_handle.\n";
		$jobs->{$job_handle} = { 
			mapper => 1, split => int($split), gearman_client => $mapper 
		};
		
	} else {
		printf(STDERR "%s\n", $mapper->error()) and die;
	}
	
	# sleep for a tenth of a sec
	select(undef, undef, undef, 0.10);
		
}


# watch the mapper processing... when one is complete, submit a job to the reducers
# each reducer worker maintains a dataset that is updated with the completion of each job

my ($return_value, $is_status_known, $running_status, $status_num, $status_denom); # gearman job status variables
my $re_job_handle;

while (1) {
	
	# stop if there are no more jobs
	last unless (keys %$jobs);

	# check each job, run reducer when a mapper is done
	foreach $job_handle (sort keys %$jobs) {
		
		# get the job status from this job's gearman client
		($return_value, $is_status_known, $running_status, $status_num, $status_denom) = 
				$jobs->{$job_handle}->{gearman_client}->job_status($job_handle);

		#print time . ' ' . $jobs->{$job_handle}->{split} . ' ' . ($jobs->{$job_handle}->{mapper} ? 'mapper' : 'reducer')  . ' -> ' 
		#	. $is_status_known  .  ' && ' . $running_status . "\n";
		
        # this job is done
	    unless ($running_status) {
			
			# this is a complete mapper job... run its reducer
			if ($jobs->{$job_handle}->{mapper}) {
				
				warn("< Done mapping " . $jobs->{$job_handle}->{split} . " with job_handle=$job_handle.\n");
			
				# submit a reducer job to be performed by gearman workers
				($ret, $re_job_handle) = $reducer->do_background( 'reducer',
					freeze ({ # workload
						'split' => int($jobs->{$job_handle}->{split}), 
						'reducer_id' => $reducer_id 
					})
				);
	
				# add this to the jobs to be monitored
				if ($ret == GEARMAN_SUCCESS) {
					print ">> Begin reducing " . $jobs->{$job_handle}->{split} . " with job_handle=$re_job_handle\n";
					$jobs->{$re_job_handle} = { reducer => 1, split => $jobs->{$job_handle}->{split}, gearman_client => $reducer };
				} else {
					printf(STDERR "%s\n", $mapper->error()) and die;
				}
			
			}
			
			# this is a complete reducer job
			if ($jobs->{$job_handle}->{reducer}) {
				print "<< Done reducing " . $jobs->{$job_handle}->{split} . " with job_handle=$job_handle.\n";
			}
			
			# delete the done job
			delete $jobs->{$job_handle};
			
	
		}

		# sleep for a tenth of a sec
			select(undef, undef, undef, 0.10);
		

	}
	

}


# aggregate all reducer datasets


print "Done with the Gearman MapReduce demo\n";

