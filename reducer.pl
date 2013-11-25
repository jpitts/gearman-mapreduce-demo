#!/usr/bin/env perl

=head1 NAME

Reducer Worker

=head1 SYNOPSIS

./reducer.pl

=head1 DESCRIPTION

Performs work submitted to the reducer gearman server by the master.pl script.

The reducer function reduces intermediate data, combining it with previously generated output data.
	1. reads the previously reduced subject list from disk
	2. reads this split's intermediate subject list from disk
	3. combines and sorts intermediate and reduced data
	2. runs the reduce function on each subject in a sort
	4. stores the reduced data to disk

The reduce function counts the values in an array of equivalent subjects.

=cut


use strict;
use warnings;

use Gearman::XS qw(:constants);
use Gearman::XS::Client;
use FreezeThaw qw(freeze thaw cmpStr safeFreeze cmpStrHard);
use Data::Dumper;


print "Fire up a Gearman Reducer Worker\n";

# set up the gearman worker
print "Connect to the Reducer gearman servers.\n";
my $worker = new Gearman::XS::Worker;
if ($worker->add_server('localhost', '4731') != GEARMAN_SUCCESS) {
	printf(STDERR "%s\n", $worker->error());
	exit(1);
}

# add the reducer function to the gearman worker
if ($worker->add_function("reducer", 0, \&reducer, {}) != GEARMAN_SUCCESS) {
	printf(STDERR "%s\n", $worker->error());
}

# process jobs
print "Begin processing Reducer jobs as they come in...\n";
while (1) {
	my $ret = $worker->work();
	if ($ret != GEARMAN_SUCCESS) {
		printf(STDERR "%s\n", $worker->error());
	}
}

# worker functions
# ---------------------------------------------------------

# reducer

sub reducer {
	my $job = shift;
	my ($workload) = thaw( $job->workload() );
	
	print "\n>> Begin reducing with workload:\n" . Dumper($workload);

	# make sure that there is an intermediate file to reduce
	unless ( -e 'intermediate/' . $workload->{split}) {
		print "No intermediate file for " . $workload->{split} . "\n" and return;
	}
	
	# load the subjects hash from this reducer worker's output file
	my @data;
	my @subjects;
	if (-e 'output/' . $workload->{reducer_id}) {
		open(my $fh, "<", 'output/' . $workload->{reducer_id}) or die "Can't open file: " . $!;
		while (my $line = <$fh>) {
			chomp $line;
			@data = split("\t", $line);
			push @subjects, { value => $data[0], key => $data[1] };
		}
	}
	
	# open the intermediate file
	# loop over the intermediates and load into an array
	open(my $fh, "<", 'intermediate/' . $workload->{split}) or die "Can't open file: " . $!;
	while (my $line = <$fh>) {
		chomp $line;
		@data = split("\t", $line);
		push @subjects, { value => $data[0], key => $data[1] };
	}
	
	# open the outout file (this will over-write)
	open (OUTPUT, '>output/' . $workload->{reducer_id});

	# loop over sorted subjects
	#	accumulate subjects of the same key
	#	reduce the values of these subjects
	#	store the reduce_output to output
	my $key;
	my $reduce_output;
	my $subjects_for_reduce = [];
	foreach my $subject (sort { $a->{key} cmp $b->{key} } @subjects) {
		
		# the key has changed... submit the accumulated subjects to reduce
		if ($key && $key ne $subject->{key}) {
			$reduce_output = &reduce($key, $subjects_for_reduce);
			
			# write to the output file
			print OUTPUT $reduce_output . "\t" . $key . "\n" or die "Can't write to file: " . $1;
			
			# reset the subject for reduce
			$subjects_for_reduce = [];
		}
		
		# accumulate like subjects
		push @$subjects_for_reduce, $subject;
		
		$key = $subject->{key};

	}
	
	# close the output file
	close OUTPUT;
	
	# close and delete the intermediate file
	close($fh);
	unlink 'intermediate/' . $workload->{split};
	
	print "Saved data to: output/" . $workload->{reducer_id}  . "\n";
	print "<< Done reducing\n";

}


=head1 UTILITY METHODS

=cut

=head2 reduce

Called by the reducer for each key in the data set. 

Accepts a key and an array of key/value pairs. Returns a value.

=cut

sub reduce {
	my $key = shift;
	my $subjects = shift;
	my $output; 
	
	# sum the values
	foreach my $subject (@$subjects) {
		$output += $subject->{value};
	}
	
	return $output;
}


