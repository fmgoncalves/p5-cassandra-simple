use strict;
use warnings;

use Data::Dumper;

use Cassandra::Simple;
use Cassandra::Composite qw/composite composite_to_array/;

use Sys::Hostname qw/hostname/;
use Time::HiRes qw/time/;

sub println {
	print @_, "\n";
}

my ( $keyspace, $column_family) = qw/simple simple/;

my $conn = Cassandra::Simple->new( server_name => '127.0.0.1',
								   keyspace    => $keyspace, );

my $present =
  grep { $_ eq $column_family } @{ [ $conn->list_keyspace_cfs($keyspace) ] };

unless ($present) {
	println "Creating $column_family in $keyspace";
	$conn->create_column_family( $keyspace, $column_family,
									 {
									   comparator_type          => 'UTF8Type',
									   key_validation_class     => 'UTF8Type',
									   default_validation_class => 'UTF8Type',
									 } );
}

my %data= map { "k".$_ => {map{ "c".$_ => "v".$_ } 1..20} } 1..100;
println "inserting";

$conn->batch_insert($column_family,\%data);
println "Sleeping 3 seconds to settle and then multigeting";
sleep 3;

my ($start,$end )= (1,3);

foreach(1..10){
	my $t0 = time;
	
	$conn->multiget($column_family, [map { "k".$_ } $start..$end]);
	
	println  "".($end-$start). " \t ". (time-$t0 ) ;
	$end += 10;
	sleep 0.1;
}

$conn->remove($column_family);
