use strict;
use warnings;

use Data::Dumper;

use Cassandra::Simple;

sub println {
	print @_, "\n";
}

my ($keyspace, $column_family) = qw/simple supersimple/;

my $conn = Cassandra::Simple->new(
	keyspace                => $keyspace,
);



#Method to test					code here		success
#insert_super					100%			100%
#get							100%			100%
#get_range						100%			100%
#get_count						100%			100%
#remove							100%			50% (Slice predicates on deletions are yet unimplemented in Cassandra)

my $present = grep { $_ eq $column_family } @{[ $conn->list_keyspace_cfs($keyspace) ]};

unless ( $present ){
	println "Creating $column_family in $keyspace";
	$conn->create_column_family($keyspace, $column_family, {column_type => 'Super'});
}

println "\$conn->insert($column_family, 'KeyA', { 'SuperColumnA' => { 'SubColumnA' => 'AAA', 'SubColumnB' => 'AAB'}, 'SuperColumnB' => { 'SubColumnA' => 'ABA', 'SubColumnB' => 'ABB'} })";
$conn->insert_super($column_family, 'KeyA', {'SuperColumnA' =>  {'SubColumnA' => 'AAA', 'SubColumnB' => 'AAB'}, 'SuperColumnB' => {'SubColumnA' => 'ABA', 'SubColumnB' => 'ABB'}, 'SuperColumnC' => {'SubColumnA' => 'ACA', 'SubColumnB' => 'ACB'}  });

println "\$conn->get($column_family, 'KeyA')";
println Dumper $conn->get($column_family, 'KeyA');
#Expected result: both supercolumns with all four subcolumns

println "\$conn->get($column_family, 'KeyA', { columns => ['SuperColumnB'] }  )";
println Dumper $conn->get($column_family, 'KeyA', { columns => ['SuperColumnB'] }  );
#Expected result: both subcolumns in SuperColumnB

println "\$conn->get($column_family, 'KeyA', { super_column => 'SuperColumnA', columns => [ 'SubColumnA' ]})";
println Dumper $conn->get($column_family, 'KeyA', { super_column => 'SuperColumnA', columns => [ 'SubColumnA' ]});
#Expected result: AAA

println "\$conn->insert_super($column_family, 'KeyB', {'SuperColumnA' =>  {'SubColumnA' => 'BAA', 'SubColumnB' => 'BAB'}, 'SuperColumnB' => {'SubColumnA' => 'BBA', 'SubColumnB' => 'BBB'} })";
$conn->insert_super($column_family, 'KeyB', {'SuperColumnA' =>  {'SubColumnA' => 'BAA', 'SubColumnB' => 'BAB'}, 'SuperColumnB' => {'SubColumnA' => 'BBA', 'SubColumnB' => 'BBB'} });

println "\$conn->get_range($column_family, {start => 'KeyA', finish => 'KeyB', column_count => 1})";
println Dumper $conn->get_range($column_family, {start => 'KeyA', finish => 'KeyB', column_count => 1});
#Expected result: 1 supercolumn and all respective subcolumns from keyA and keyB

println "\$conn->get_range($column_family, {super_column => 'SuperColumnA', start => 'SubColumnA', finish => 'SubColumnB'})";
println Dumper $conn->get_range($column_family, {super_column => 'SuperColumnA', start_column => 'SubColumnA', finish_column => 'SubColumnB'});
#Expected result: supercolumnA's subcolumnA and subcolumnB from all keys

println "\$conn->get_count($column_family, 'KeyA')";
println Dumper $conn->get_count($column_family, 'KeyA');
#Expected result: 3

##NOT IMPLEMENTED IN CASSANDRA YET
#println "\$conn->remove($column_family, ['KeyA'], {super_column => 'SuperColumnC'})";
#println Dumper $conn->remove($column_family, ['KeyA'], {super_column => 'SuperColumnC'});
#
#println "\$conn->get_count($column_family, 'KeyA')";
#println Dumper $conn->get_count($column_family, 'KeyA');
##Expected result: 2

println "\$conn->get_count($column_family, 'KeyA', {super_column => 'SuperColumnB'})";
println Dumper $conn->get_count($column_family, 'KeyA', {super_column => 'SuperColumnB'});
#Expected result: 2

println "\$conn->remove($column_family)";
println Dumper $conn->remove($column_family);
