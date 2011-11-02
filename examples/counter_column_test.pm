use strict;
use warnings;

use Data::Dumper;

use Cassandra::Simple;
use Cassandra::Composite qw/composite composite_to_array/;

use Sys::Hostname qw/hostname/;

sub println {
	print @_, "\n";
}

my ( $keyspace, $column_family) = qw/simple simplecounter/;

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
									   default_validation_class => 'CounterColumnType',
									 } );
}


#Method to test					code here		success
#add											100%						100%
#remove_counter				100%						100%
#get												100%						100%


println "\$conn->add($column_family, 'ChaveA', 'ColunaA')";
println Dumper $conn->add($column_family, 'ChaveA', 'ColunaA');

println "\$conn->get($column_family, 'ChaveA', {columns => ['ColunaA']})";
println Dumper $conn->get($column_family, 'ChaveA', {columns => ['ColunaA']});
#Expected result: ColunaA -> 1

println "\$conn->add($column_family, 'ChaveA', 'ColunaA',10)";
println Dumper $conn->add($column_family, 'ChaveA', 'ColunaA', 10);

println "\$conn->get($column_family, 'ChaveA', {columns => ['ColunaA']})";
println Dumper $conn->get($column_family, 'ChaveA', {columns => ['ColunaA']});
#Expected result: ColunaA -> 11

println "\$conn->remove_counter($column_family, 'ChaveA', 'ColunaA')";
println Dumper $conn->remove_counter($column_family, 'ChaveA', 'ColunaA');

