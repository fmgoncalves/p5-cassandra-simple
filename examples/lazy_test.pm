#! /usr/bin/perl -l

use strict;
use warnings;

use Carp::Always;

use Data::Dumper;
use Cassandra::Simple;

my ( $keyspace, $column_family ) =
  qw/simple simple/;

my $sys_conn = Cassandra::Simple->new();
unless ( grep { $_ eq $keyspace } @{ $sys_conn->list_keyspaces() } ) {
	print "Creating keyspace $keyspace";
	$sys_conn->create_keyspace( keyspace => $keyspace);
}

my $conn = Cassandra::Simple->new( keyspace => $keyspace, );

my $present =
  grep { $_ eq $column_family } @{ $conn->list_keyspace_cfs( keyspace => $keyspace) };

unless ($present) {
	print "Creating $column_family in $keyspace";
	$conn->create_column_family(
								column_family            => $column_family,
								comparator_type          => 'UTF8Type',
								key_validation_class     => 'UTF8Type',
								default_validation_class => 'UTF8Type',
	);
}


print "\$conn->create_index( column_family => $column_family, columns => ['age'] )";
print Dumper $conn->create_index( column_family => $column_family, columns => ['age'] );

print
"\$conn->batch_insert( column_Family => $column_family, rows => { 'whisky1' => { 'age' => 12 }, 'whisky2' => { 'age' => 12 }, 'whisky3' => { 'age' => 15 }, 'whisky4' => { 'age' => 12 } })";
print Dumper $conn->batch_insert(
									column_family => $column_family,
									rows => {
									   'whisky1' => { 'age' => 12 },
									   'whisky2' => { 'age' => 12 },
									   'whisky3' => { 'age' => 15 },
									   'whisky4' => { 'age' => 12 },
									   'whisky5' => { 'age' => 12 },
									   'whisky6' => { 'age' => 12 },
									   'whisky7' => { 'age' => 12 },
									   'whisky8' => { 'age' => 12 },
									   'whisky9' => { 'age' => 12 },
									   'whisky10' => { 'age' => 12 },
									   'whisky11' => { 'age' => 17 },
									   'whisky12' => { 'age' => 12 }
									}
);



print '$row_query = $conn->get_indexed_slices( column_family => $column_family, expression_list => [ [ \'age\' => \'12\' ] ] )';
my $row_query = $conn->lazy_query('get_indexed_slices', column_family => $column_family, expression_list => [ [ 'age' => '12' ] ] );

my $i = 0;
while( my $res = $row_query->run(1) ){ # the value passed onto run is the number of results intended in this iteration
	#print "Res $i: ".Dumper $res;
	$i++;
}
die "LazyQuery didn't traverse the keys correctly." unless $i == 10;
print "lazy success\n";

print '$conn->insert( column_family => $column_family, key => \'ChaveA\', columns =>{ \'ColunaA1\' => \'ValorA1\', \'ColunaA2\' => \'ValorA2\', \'ColunaA3\' => \'ValorA3\', \'ColunaA4\' => \'ValorA4\', \'ColunaA5\' => \'ValorA5\', \'ColunaA6\' => \'ValorA6\' } )';
print Dumper $conn->insert( column_family => $column_family, key => 'ChaveA',
			   columns =>{ 'ColunaA1' => 'ValorA1', 'ColunaA2' => 'ValorA2', 'ColunaA3' => 'ValorA3', 'ColunaA4' => 'ValorA4', 'ColunaA5' => 'ValorA5', 'ColunaA6' => 'ValorA6' } );

print '$col_query = $conn->lazy_query(\'get\', column_family => $column_family, key => \'ChaveA\'';
my $col_query = $conn->lazy_query('get', column_family => $column_family, key => 'ChaveA');
$i=0;
while( my $res = $col_query->run(2) ){
        #print "Res $i: ".join("\n", keys %$res,'');
        $i++;
}
die "LazyQuery didn't traverse the keys correctly." unless $i == 3;
print "lazy success\n";

print Dumper "\$conn->drop_keyspace()";
print Dumper $sys_conn->drop_keyspace(keyspace => $keyspace);