#!/usr/bin/env perl

=head1 NAME

Mapper Worker

=head1 SYNOPSIS

	./mapper.pl

=head1 DESCRIPTION

Performs work submitted to the mapper gearman server by the master.pl script.

The mapper process the files in a split, generating an intermediate set of data.
	1. creates an intermediate file named after the split
	2. submits each corpus file from the split to the map function
	3. stores the map output to the intermediate file

The map function runs map for each file name in the split.
	1. opens the file
	2. uses a pattern and regex to generate an array of subjects
	3. returns an array representing the order of occurence

=cut

use strict;
use warnings;

use Gearman::XS qw(:constants);
use Gearman::XS::Client;
use FreezeThaw qw(freeze thaw cmpStr safeFreeze cmpStrHard);
use Data::Dumper;


print "Fire up a Gearman Mapper Worker\n";

# set up the gearman worker
print "Connect to the Mapper gearman servers.\n";
my $worker = new Gearman::XS::Worker;
if ($worker->add_server('localhost', '4730') != GEARMAN_SUCCESS) {
	printf(STDERR "%s\n", $worker->error());
	exit(1);
}

# add the mapper function to the gearman worker
if ($worker->add_function("mapper", 0, \&mapper, {}) != GEARMAN_SUCCESS) {
	printf(STDERR "%s\n", $worker->error());
}

# process jobs
print "Begin processing Mapper jobs as they come in...\n";
while (1) {
	my $ret = $worker->work();
	if ($ret != GEARMAN_SUCCESS) {
		printf(STDERR "%s\n", $worker->error());
	}
}


=head1 WORKER METHODS

=cut

=head2 mapper

Called by the gearman server once a mapper job is submitted by the master.

=cut

sub mapper {
	my $job = shift;
	my ($workload) = thaw( $job->workload() );
	
	print "\n> Begin mapping with workload:\n" . Dumper($workload);
	
	# look into the corpus directory and submit files from the split to the map function
	open (INTERMEDIATE, '>>intermediate/' . $workload->{split});
	opendir(DIR, "corpus");
	foreach my $file (readdir(DIR)) {
		next unless ($file =~ m/^$workload->{split}\-/);
		
		# append the map function output to the intermediate file
		foreach (&map($file)) {
			print INTERMEDIATE 1 . "\t" . $_ . "\n" or die "Can't write to file: " . $1;;
		}
		# the format is: COUNT, TAB, KEY
		
	}
	closedir DIR;
	close INTERMEDIATE;
	
	print "Saved data to: intermediate/" . $workload->{split}  . "\n";
	print "< Done mapping.\n";
	
	return 1;

}

=head1 UTILITY METHODS

=cut

=head2 map

Called by the mapper for each file in the split. 

Accepts a file name. Returns an array.

=cut

sub map {
	my $file_name = shift;
	my @output;
	my $text;
	
	# read the contents of the file
	open(my $fh, "<", 'corpus/' . $file_name) or die "Can't open file: " . $!;
	while (my $line = <$fh>) {
		$text .= $line . "\n";
	}
	
	# very rudimentary pattern match for capitalized subjects
	my $pattern = '(?:[[:upper:]][[:alpha:]]+)';
	my @matches = ( $text =~ /\s(${pattern}(?:\s+${pattern})*)/g );
	
	# clean up the data
	foreach my $subj (@matches) {
		$subj =~ s/\s+/ /g;
		push @output, $subj;
	}
	
	return @output;
}


