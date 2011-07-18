use strict;
use warnings;

use Data::Dumper;

use Cassandra::Simple;

sub println {
	print @_, "\n";
}

my ( $keyspace, $column_family ) = qw/simple simple/;

my $conn = Cassandra::Simple->new( keyspace => $keyspace, );

my $present =
  grep { $_ eq $column_family } @{[ $conn->list_keyspace_cfs($keyspace) ]};

unless ($present) {
	println "Creating $column_family in $keyspace";
	$conn->create_column_family( $keyspace, $column_family, 1 );
}

println "\$conn->insert($column_family, 'DyingKey', { 'C1' => 'Dead1' , 'C2' => 'Dead2' }, { ttl => 20 } )";
$conn->insert($column_family, 'DyingKey', { 'C1' => 'Dead1' , 'C2' => 'Dead2' }, { ttl => 20 } );

println "\$conn->insert($column_family, 'DyingKey', { 'C3' => 'Dead3' } , { ttl => 30 })";
$conn->insert($column_family, 'DyingKey', { 'C3' => 'Dead3' } , { ttl => 30 });

println "\$conn->get($column_family, 'DyingKey')";
println Dumper $conn->get($column_family, 'DyingKey');
#Expected result: C1, C2 and C3

println "sleep(22)";
for (my $i=22; $i >= 0; $i--){
	print "$i . . ";
	sleep(1);
}
println "\n";

println "\$conn->get($column_family, 'DyingKey')";
println Dumper $conn->get($column_family, 'DyingKey');
#Expected result: C3

println "sleep(12)";
for (my $i=12; $i >= 0; $i--){
	print "$i . . ";
	sleep(1);
}
println "\n";

println "\$conn->get($column_family, 'DyingKey')";
println Dumper $conn->get($column_family, 'DyingKey');
#Expected result: none

println "\$conn->remove($column_family)";
println Dumper $conn->remove($column_family);