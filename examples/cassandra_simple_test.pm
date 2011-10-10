use strict;
use warnings;

use Data::Dumper;

use Cassandra::Simple;
use Cassandra::Composite qw/composite/;

use Sys::Hostname qw/hostname/;

sub println {
	print @_, "\n";
}

my ( $keyspace, $column_family ) = qw/simple simple/;

my $conn = Cassandra::Simple->new( server_name => '127.0.0.1',
								   keyspace    => $keyspace, );

my $present =
  grep { $_ eq $column_family } @{ [ $conn->list_keyspace_cfs($keyspace) ] };

unless ($present) {
	println "Creating $column_family in $keyspace";
	$conn->create_column_family( $keyspace, $column_family );
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
#composite get				100%			100%
#composite insert		100%			100%

println
"\$conn->insert($column_family, 'ChaveA', { 'ColunaA1' => 'ValorA1' , 'ColunaA2' => 'ValorA2' } )";
$conn->insert( $column_family, 'ChaveA',
			   { 'ColunaA1' => 'ValorA1', 'ColunaA2' => 'ValorA2' } );

println
  "\$conn->get($column_family, 'ChaveA', { columns => [ qw/ColunaA1/ ] })";
println Dumper $conn->get( $column_family, 'ChaveA',
						   { columns => [qw/ColunaA1/] } );

#Expected result: ValorA1

println "\$conn->get($column_family, 'ChaveA')";
println Dumper $conn->get( $column_family, 'ChaveA' );

#Expected result: { 'ColunaA1' => 'ValorA1', 'ColunaA2' => 'ValorA2' }

println
"\$conn->get($column_family, 'ChaveA', { column_count => 1, column_reversed => 1 })";
println Dumper $conn->get( $column_family, 'ChaveA',
						   { column_count => 1, column_reversed => 1 } );

#Expected result: only one column, the last given by get($column_family, 'ChaveA')

println
"\$conn->batch_insert($column_family, { 'ChaveB' => {'ColunaB1' => 'ValorB1' , 'ColunaB2' => 'ValorB2' }, 'ChaveC' => { 'ColunaC1' => 'ValorC1' , 'ColunaC2' => 'ValorC2' } })";
println Dumper $conn->batch_insert(
									$column_family,
									{
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

println "\$conn->multiget($column_family, [ qw/ChaveA ChaveC/ ])";
println Dumper $conn->multiget( $column_family, [qw/ChaveA ChaveC/] );

#Expected result: all content from ChaveA and ChaveC

println
"\$conn->get_range($column_family, { start=> 'ChaveA', finish => 'ChaveB', column_count => 1 })";
println Dumper $conn->get_range(
								 $column_family,
								 {
									start        => 'ChaveA',
									finish       => 'ChaveB',
									column_count => 1
								 }
);

#Expected result: Depends on key order inside Cassandra. Probably only these 2 keys are returned with 1 column each.

println "\$conn->create_index($keyspace, $column_family, 'age')";
println Dumper $conn->create_index( $keyspace, $column_family, 'age' );

println
"\$conn->batch_insert($column_family, { 'whisky1' => { 'age' => 12 }, 'whisky2' => { 'age' => 12 }, 'whisky3' => { 'age' => 15 }, 'whisky4' => { 'age' => 12 } })";
println Dumper $conn->batch_insert(
									$column_family,
									{
									   'whisky1' => { 'age' => 12 },
									   'whisky2' => { 'age' => 12 },
									   'whisky3' => { 'age' => 15 },
									   'whisky4' => { 'age' => 12 }
									}
);

println
"\$conn->get_indexed_slices($column_family, { expression_list => [ [ 'age' => '12' ] ] })";
println Dumper $conn->get_indexed_slices( $column_family,
								 { expression_list => [ [ 'age' => '12' ] ] } );

#Expected result: Rows whisky1, whisky2, whisky4

println
  "\$conn->remove($column_family, [ 'ChaveA' ], { columns => [ 'ColunaA1' ]})";
println Dumper $conn->remove( $column_family, ['ChaveA'],
							  { columns => ['ColunaA1'] } );

println "\$conn->multiget_count($column_family, ['ChaveA', 'ChaveB'])";
println Dumper $conn->multiget_count( $column_family, [ 'ChaveA', 'ChaveB' ] );

#Expected result: ChaveA -> 1, ChaveB -> 2

println "\$conn->get_count($column_family, 'whisky2')";
println Dumper $conn->get_count( $column_family, 'whisky2' );

#Expected result: 1

println "\$conn->remove($column_family, 'whisky2')";
println Dumper $conn->remove( $column_family, 'whisky2' );

println "\$conn->get($column_family, 'whisky2')";
println Dumper $conn->get( $column_family, 'whisky2' );

println "\$conn->remove($column_family)";
println Dumper $conn->remove($column_family);

println "\$conn->get_range($column_family)";
println Dumper $conn->get_range($column_family);

#Expected result: empty list

println "\$conn->ring('simple')";
println Dumper $conn->ring('simple');

$column_family = "TCF";
println
"\$conn->insert(  'TCF',  'hello',  {  composite( 'a','en') => 'world' ,  composite('a','pt') => 'mundo'  } )";
println Dumper $conn->insert(
							  $column_family, "hello",
							  {
								 composite( "a", "en" ) => "world",
								 composite( "a", "pt" ) => "mundo"
							  }
);

println
  "\$conn->get( 'TCF',  'hello', { columns => [ composite('a', 'pt' ) ] } )";
println Dumper $conn->get( $column_family, "hello",
						   { columns => [ composite( "a", "pt" ) ] } );

println Dumper "\$conn->remove($column_family)";
println Dumper $conn->remove($column_family);
