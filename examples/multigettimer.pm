#! /usr/bin/perl -l

use strict;
use warnings;

use Data::Dumper;

use Cassandra::Simple;
use Cassandra::Composite qw/composite composite_to_array/;

use Time::HiRes qw/time/;

my ( $keyspace, $column_family) = qw/simple multigetsimple/;

my $sys_conn = Cassandra::Simple->new();
unless ( grep { $_ eq $keyspace } @{ $sys_conn->list_keyspaces() } ) {
	print "Creating keyspace $keyspace";
	$sys_conn->create_keyspace(keyspace => $keyspace);
}

my $conn = Cassandra::Simple->new( keyspace => $keyspace, );

my $present =
  grep { $_ eq $column_family } @{ $conn->list_keyspace_cfs() };

unless ($present) {
	print "Creating $column_family in $keyspace";
	$conn->create_column_family( column_family => $column_family,
								comparator_type          => 'UTF8Type',
								key_validation_class     => 'UTF8Type',
								default_validation_class => 'UTF8Type',
							);
}

my %data= map { "k".$_ => {map{ "c".$_ => "v".$_ } 1..20} } 1..1000;
print "inserting";

$conn->batch_insert(column_family => $column_family, rows => \%data);
print "Sleeping 3 seconds to settle and then multigeting";
sleep 3;

my ($start,$end )= (1,3);

foreach(1..20){
	my $t0 = time;
	
	$conn->multiget(column_family => $column_family, 'keys' => [map { "k".$_ } $start..$end]);
	
	print  "".($end-$start). " \t ". (time-$t0 ) ;
	$end += 10;
	sleep 0.1;
}

$conn->remove(column_family => $column_family);


print Dumper "\$conn->drop_keyspace($keyspace)";
print Dumper $sys_conn->drop_keyspace(keyspace => $keyspace);