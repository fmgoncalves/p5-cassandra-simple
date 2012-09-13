#! /usr/bin/perl -l

use strict;
use warnings;

use Carp::Always;

use Data::Dumper;
use Cassandra::Simple;
use Cassandra::Composite qw/composite composite_to_array/;

my ( $keyspace, $column_family, $composite_column_family ) =
  qw/simple simple simplecomposite/;

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

$present =
  grep { $_ eq $composite_column_family }
  @{ $conn->list_keyspace_cfs(keyspace => $keyspace) };

unless ($present) {
	print "Creating $composite_column_family in $keyspace";
	$conn->create_column_family(
						column_family => $composite_column_family,
						comparator_type => 'CompositeType(UTF8Type,UTF8Type)',
						key_validation_class     => 'UTF8Type',
						default_validation_class => 'UTF8Type',
	);
}

#Method to test					code here		success
#get							100%			100%
#multiget						100%			100%
#get_count						100%			100%
#multiget_count					100%			100%
#get_range						100%			100%
#get_indexed_slices				100%			100%
#insert 						100%			100%
#batch_insert					100%			100%
#remove							100%			100%
#composite get					100%			100%
#composite insert				100%			100%

print
"\$conn->insert( column_family => $column_family, key => 'ChaveA',
			   columns =>{ 'ColunaA1' => 'ValorA1', 'ColunaA2' => 'ValorA2' } );";
$conn->insert( column_family => $column_family, key => 'ChaveA',
			   columns =>{ 'ColunaA1' => 'ValorA1', 'ColunaA2' => 'ValorA2' } );

print
  "\$conn->get( column_family => $column_family, key => 'ChaveA', columns => [qw/ColunaA1/] )";
print Dumper $conn->get( column_family => $column_family, key => 'ChaveA', columns => [qw/ColunaA1/] );

#Expected result: ValorA1

print "\$conn->get( column_family => $column_family, key => 'ChaveA' )";
print Dumper $conn->get( column_family => $column_family, key => 'ChaveA' );

#Expected result: { 'ColunaA1' => 'ValorA1', 'ColunaA2' => 'ValorA2' }

print
"\$conn->get( column_family => $column_family, key => 'ChaveA', column_count => 1, column_reversed => 1 )";
print Dumper $conn->get( column_family => $column_family, key => 'ChaveA', column_count => 1, column_reversed => 1 );

#Expected result: only one column, the last given by get($column_family, 'ChaveA')

print
"\$conn->batch_insert(
									column_family => $column_family,
									rows => {
									   'ChaveB' => {
													 'ColunaB1' => 'ValorB1',
													 'ColunaB2' => 'ValorB2'
									   },
									   'ChaveC' => {
													 'ColunaC1' => 'ValorC1',
													 'ColunaC2' => 'ValorC2'
									   }
									}
);";
print Dumper $conn->batch_insert(
									column_family => $column_family,
									rows => {
									   'ChaveB' => {
													 'ColunaB1' => 'ValorB1',
													 'ColunaB2' => 'ValorB2'
									   },
									   'ChaveC' => {
													 'ColunaC1' => 'ValorC1',
													 'ColunaC2' => 'ValorC2'
									   }
									}
);

print "\$conn->multiget( column_family => $column_family, 'keys' => [qw/ChaveA ChaveC/] )";
print Dumper $conn->multiget( column_family => $column_family, 'keys' => [qw/ChaveA ChaveC/] );

#Expected result: all content from ChaveA and ChaveC


my $query = $conn->lazy_query('get_range', column_family => $column_family);

my $i = 0;
while( my $res = $query->run(1) ){
	$i++;
}
die "LazyQuery didn't traverse the keys correctly." unless $i == 3;


print
"\$conn->get_range( column_family => $column_family, start => 'ChaveA', finish => 'ChaveB', column_count => 1 );";
print Dumper $conn->get_range(
								column_family => $column_family,
								start        => 'ChaveA',
								finish       => 'ChaveB',
								column_count => 1
);

#Expected result: Depends on key order inside Cassandra. Probably only these 2 keys are returned with 1 column each.

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
									   'whisky4' => { 'age' => 12 }
									}
);

print
"\$conn->get_indexed_slices( column_family => $column_family, expression_list => [ [ 'age' => '12' ] ] )";
print Dumper $conn->get_indexed_slices( column_family => $column_family, expression_list => [ [ 'age' => '12' ] ] );

#Expected result: Rows whisky1, whisky2, whisky4

print
"\$conn->get_indexed_slices(column_family => $column_family, expression_list => [ [ 'age' , '=' , '12' ] ])";
print Dumper $conn->get_indexed_slices( column_family => $column_family, expression_list => [ [ 'age', '=', '12' ] ] );

#Expected result: Rows whisky1, whisky2, whisky4

print
  "\$conn->remove( column_family => $column_family, keys => [ 'ChaveA' ], columns => [ 'ColunaA1' ])";
print Dumper $conn->remove( column_family =>  $column_family, 'keys' => ['ChaveA'], columns => ['ColunaA1'] );

print "\$conn->multiget_count($column_family, ['ChaveA', 'ChaveB'])";
print Dumper $conn->multiget_count( column_family => $column_family, 'keys' => [ 'ChaveA', 'ChaveB' ] );

#Expected result: ChaveA -> 1, ChaveB -> 2

print "\$conn->get_count($column_family, 'whisky2')";
print Dumper $conn->get_count( column_family => $column_family, key => 'whisky2' );

#Expected result: 1

print "\$conn->remove($column_family, 'whisky2')";
print Dumper $conn->remove( column_family => $column_family, 'keys' => ['whisky2'] );

print "\$conn->get($column_family, 'whisky2')";
print Dumper $conn->get( column_family => $column_family, key => 'whisky2' );

print "\$conn->remove($column_family)";
print Dumper $conn->remove(column_family => $column_family);

print "\$conn->get_range($column_family)";
print Dumper $conn->get_range(column_family => $column_family);

#Expected result: empty list

print "\$conn->ring(keyspace => 'simple')";
print Dumper $conn->ring(keyspace => 'simple');
print "\$conn->ring()";
print Dumper $conn->ring();

print
"\$conn->insert( column_family => $composite_column_family, key => 'hello',
							  columns => {
								 composite( 'a', 'en' ) => 'world',
								 composite( 'a', 'pt' ) => 'mundo'
							  }
)";
print Dumper $conn->insert(
							  column_family => $composite_column_family,
							  key => "hello",
							  columns => {
								 composite( "a", "en" ) => "world",
								 composite( "a", "pt" ) => "mundo"
							  }
);

print
"\$conn->get(column_family => $composite_column_family, key => 'hello', columns => [ composite('a', 'pt' ) ] )";
my $x = $conn->get( column_family => $composite_column_family,  key => "hello",
					columns => [ composite( "a", "pt" ) ]  );
my %aux = map { ( join ':', @{ composite_to_array($_) } ) => $x->{$_} } keys %$x;
print Dumper \%aux;

print Dumper "\$conn->remove(column_family => $composite_column_family)";
print Dumper $conn->remove(column_family => $composite_column_family);

print Dumper "\$conn->drop_keyspace()";
print Dumper $sys_conn->drop_keyspace(keyspace => $keyspace);
