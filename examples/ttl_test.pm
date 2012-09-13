#! /usr/bin/perl -l

use strict;
use warnings;

use Data::Dumper;

use Cassandra::Simple;

my ( $keyspace, $column_family ) = qw/simple ttlsimple/;

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
	$conn->create_column_family( column_family => $column_family );
}

print "\$conn->insert(column_family => $column_family, key => 'DyingKey', columns => { 'C1' => 'Dead1' , 'C2' => 'Dead2' }, ttl => 20 )";
$conn->insert(column_family => $column_family, key => 'DyingKey', columns => { 'C1' => 'Dead1' , 'C2' => 'Dead2' }, ttl => 20 );

print "\$conn->insert(column_family => $column_family, key => 'DyingKey', columns => { 'C3' => 'Dead3' } , ttl => 30 )";
$conn->insert(column_family => $column_family, key => 'DyingKey', columns => { 'C3' => 'Dead3' } , ttl => 30 );

print "\$conn->get(column_family => $column_family, key => 'DyingKey')";
print Dumper $conn->get(column_family => $column_family, key => 'DyingKey');
#Expected result: C1, C2 and C3

print "sleep(22)";
for (my $i=22; $i >= 0; $i--){
	print "$i . . ";
	sleep(1);
}
print "\n";

print "\$conn->get(column_family => $column_family, key => 'DyingKey')";
print Dumper $conn->get(column_family => $column_family, key => 'DyingKey');
#Expected result: C3

print "sleep(12)";
for (my $i=12; $i >= 0; $i--){
	print "$i . . ";
	sleep(1);
}
print "\n";

print "\$conn->get(column_family => $column_family, key => 'DyingKey')";
print Dumper $conn->get(column_family => $column_family, key => 'DyingKey');
#Expected result: none

print "\$conn->remove(column_family => $column_family)";
print Dumper $conn->remove(column_family => $column_family);


print Dumper "\$conn->drop_keyspace(keyspace => $keyspace)";
print Dumper $sys_conn->drop_keyspace(keyspace => $keyspace);