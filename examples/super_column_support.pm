#! /usr/bin/perl -l

use strict;
use warnings;

use Data::Dumper;

use Cassandra::Simple;

my ($keyspace, $column_family) = qw/simple supersimple/;

my $sys_conn = Cassandra::Simple->new();
unless ( grep { $_ eq $keyspace } @{ $sys_conn->list_keyspaces() } ) {
	print "Creating keyspace $keyspace";
	$sys_conn->create_keyspace(keyspace => $keyspace);
}

my $conn = Cassandra::Simple->new(
	keyspace => $keyspace,
);


#Method to test					code here		success
#insert_super					100%			100%
#get							100%			100%
#get_range						100%			100%
#get_count						100%			100%
#remove							100%			50% (Slice predicates on deletions are yet unimplemented in Cassandra)

my $present = grep { $_ eq $column_family } @{ $conn->list_keyspace_cfs() };

unless ( $present ){
	print "Creating $column_family in $keyspace";
	$conn->create_column_family(column_family => $column_family, column_type => 'Super');
}

print "\$conn->insert($column_family, 'KeyA', { 'SuperColumnA' => { 'SubColumnA' => 'AAA', 'SubColumnB' => 'AAB'}, 'SuperColumnB' => { 'SubColumnA' => 'ABA', 'SubColumnB' => 'ABB'} })";
$conn->insert_super(column_family => $column_family, key => 'KeyA', columns => {'SuperColumnA' =>  {'SubColumnA' => 'AAA', 'SubColumnB' => 'AAB'}, 'SuperColumnB' => {'SubColumnA' => 'ABA', 'SubColumnB' => 'ABB'}, 'SuperColumnC' => {'SubColumnA' => 'ACA', 'SubColumnB' => 'ACB'}  });

print "\$conn->get($column_family, 'KeyA')";
print Dumper $conn->get(column_family => $column_family, key => 'KeyA');
#Expected result: both supercolumns with all four subcolumns

print "\$conn->get($column_family, 'KeyA', { columns => ['SuperColumnB'] }  )";
print Dumper $conn->get(column_family => $column_family, key => 'KeyA', columns => ['SuperColumnB'] );
#Expected result: both subcolumns in SuperColumnB

print "\$conn->get($column_family, 'KeyA', { super_column => 'SuperColumnA', columns => [ 'SubColumnA' ]})";
print Dumper $conn->get(column_family => $column_family, key => 'KeyA', super_column => 'SuperColumnA', columns => [ 'SubColumnA' ]);
#Expected result: AAA

print "\$conn->batch_insert($column_family, {'KeyB' => {'SuperColumnA' =>  {'SubColumnA' => 'BAA', 'SubColumnB' => 'BAB'}, 'SuperColumnB' => {'SubColumnA' => 'BBA', 'SubColumnB' => 'BBB'} }})";
$conn->batch_insert(column_family => $column_family, rows => {'KeyB' => {'SuperColumnA' =>  {'SubColumnA' => 'BAA', 'SubColumnB' => 'BAB'}, 'SuperColumnB' => {'SubColumnA' => 'BBA', 'SubColumnB' => 'BBB'} }});

print "\$conn->get_range($column_family, {start => 'KeyA', finish => 'KeyB', column_count => 1})";
print Dumper $conn->get_range(column_family => $column_family, start => 'KeyA', finish => 'KeyB', column_count => 1);
#Expected result: 1 supercolumn and all respective subcolumns from keyA and keyB

print "\$conn->get_range($column_family, {super_column => 'SuperColumnA', start => 'SubColumnA', finish => 'SubColumnB'})";
print Dumper $conn->get_range(column_family => $column_family, super_column => 'SuperColumnA', start_column => 'SubColumnA', finish_column => 'SubColumnB');
#Expected result: supercolumnA's subcolumnA and subcolumnB from all keys

print "\$conn->get_count($column_family, 'KeyA')";
print Dumper $conn->get_count(column_family => $column_family, key => 'KeyA');
#Expected result: 3

##NOT IMPLEMENTED IN CASSANDRA YET
#print "\$conn->remove($column_family, ['KeyA'], {super_column => 'SuperColumnC'})";
#print Dumper $conn->remove($column_family, ['KeyA'], {super_column => 'SuperColumnC'});
#
#print "\$conn->get_count($column_family, 'KeyA')";
#print Dumper $conn->get_count($column_family, 'KeyA');
##Expected result: 2

print "\$conn->get_count($column_family, 'KeyA', {super_column => 'SuperColumnB'})";
print Dumper $conn->get_count(column_family => $column_family, key => 'KeyA', super_column => 'SuperColumnB');
#Expected result: 2

print "\$conn->remove($column_family)";
print Dumper $conn->remove(column_family => $column_family);


print Dumper "\$conn->drop_keyspace($keyspace)";
print Dumper $sys_conn->drop_keyspace(keyspace => $keyspace);
