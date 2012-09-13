#! /usr/bin/perl -l

use strict;
use warnings;

use Data::Dumper;

use Cassandra::Simple;
use Cassandra::Composite qw/composite composite_to_array/;

my ( $keyspace, $column_family) = qw/simple countersimple/;

my $sys_conn = Cassandra::Simple->new();
unless ( grep { $_ eq $keyspace } @{ $sys_conn->list_keyspaces() } ) {
	print "Creating keyspace $keyspace";
	$sys_conn->create_keyspace(keyspace => $keyspace);
}

my $conn = Cassandra::Simple->new( keyspace  => $keyspace, );

my $present =
  grep { $_ eq $column_family } @{ $conn->list_keyspace_cfs() };

unless ($present) {
	print "Creating $column_family in $keyspace";
	$conn->create_column_family( column_family =>  $column_family,
								comparator_type          => 'UTF8Type',
								key_validation_class     => 'UTF8Type',
								default_validation_class => 'CounterColumnType',
							);
}


#Method to test					code here		success
#add											100%						100%
#remove_counter				100%						100%
#get												100%						100%


print "\$conn->add($column_family, 'ChaveA', 'ColunaA')";
print Dumper $conn->add(column_family => $column_family, key => 'ChaveA', column => 'ColunaA');

print "\$conn->get($column_family, 'ChaveA', {columns => ['ColunaA']})";
print Dumper $conn->get(column_family => $column_family, key => 'ChaveA', columns => ['ColunaA']);
#Expected result: ColunaA -> 1

print "\$conn->add($column_family, 'ChaveA', 'ColunaA', 0)";
print Dumper $conn->add(column_family => $column_family, key => 'ChaveA', column => 'ColunaA', value => 0);

print "\$conn->get($column_family, 'ChaveA', {columns => ['ColunaA']})";
print Dumper $conn->get(column_family => $column_family, key => 'ChaveA', columns => ['ColunaA']);
#Expected result: ColunaA -> 1

print "\$conn->batch_add($column_family, {'ChaveA'=> {'ColunaA'=> 10}})";
print Dumper $conn->batch_add(column_family => $column_family, rows => {'ChaveA'=> {'ColunaA'=> 10}});

print "\$conn->get($column_family, 'ChaveA', {columns => ['ColunaA']})";
print Dumper $conn->get(column_family => $column_family, key => 'ChaveA', columns => ['ColunaA']);
#Expected result: ColunaA -> 11

print "\$conn->remove_counter($column_family, 'ChaveA', 'ColunaA')";
print Dumper $conn->remove_counter(column_family => $column_family, key => 'ChaveA', column => 'ColunaA');

print Dumper "\$conn->drop_keyspace($keyspace)";
print Dumper $sys_conn->drop_keyspace(keyspace => $keyspace);