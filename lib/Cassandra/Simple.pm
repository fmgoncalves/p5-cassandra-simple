package Cassandra::Simple;

=pod

=encoding utf8

=head1 NAME

Cassandra::Simple

=head1 VERSION

version 0.1

=head1 DESCRIPTION

Easy to use, Perl oriented client interface to Apache Cassandra.

This module attempts to abstract the underlying Thrift methods as much as possible to allow any Perl developer a small learning curve when using Cassandra.


=head1 SYNOPSYS

	my ($keyspace, $column_family) = qw/simple simple/;
	
	my $conn = Cassandra::Simple->new(keyspace => $keyspace,);
	
	$conn->create_column_family( $keyspace, $column_family);
	
	$conn->insert($column_family, 'KeyA', [ [ 'ColumnA' => 'AA' ], [ 'ColumnB' => 'AB' ] ] );
	
	$conn->get($column_family, 'KeyA');
	$conn->get($column_family, 'KeyA', { columns => [ qw/ColumnA/ ] });
	$conn->get($column_family, 'KeyA', { column_count => 1, column_reversed => 1 });
	
	$conn->batch_insert($column_family, { 'KeyB' => [ [ 'ColumnA' => 'BA' ] , [ 'ColumnB' => 'BB' ] ], 'KeyC' => [ [ 'ColumnA' => 'CA' ] , [ 'ColumnD' => 'CD' ] ] });
	
	$conn->multiget($column_family, [qw/KeyA KeyC/]);
	
	$conn->get_range($column_family, { start=> 'KeyA', finish => 'KeyB', column_count => 1 });
	$conn->get_range($column_family);
	
	$conn->get_indexed_slices($column_family, { expression_list => [ [ 'ColumnA' => 'BA' ] ] });
	
	$conn->remove($column_family, [ 'KeyA' ], { columns => [ 'ColumnA' ]});
	$conn->remove($column_family, [ 'KeyA' ]);
	$conn->remove($column_family);
	
	$conn->get_count($column_family, 'KeyA');
	$conn->multiget_count($column_family, [ 'KeyB', 'KeyC' ]);


=cut

use strict;
use warnings;

use Data::Dumper;

use Any::Moose;
has 'pool' => ( is => 'rw', isa => 'Cassandra::Pool', lazy_build => 1 );
has 'consistency_level_read'  => ( is => 'rw', isa => 'Str', default => 'ONE' );
has 'consistency_level_write' => ( is => 'rw', isa => 'Str', default => 'ONE' );
has 'keyspace'    => ( is => 'rw', isa => 'Str' );
has 'password'    => ( is => 'rw', isa => 'Str', default => '' );
has 'server_name' => ( is => 'rw', isa => 'Str', default => '127.0.0.1' );
has 'server_port' => ( is => 'rw', isa => 'Int', default => 9160 );

has 'username' => ( is => 'rw', isa => 'Str', default => '' );

use 5.010;
use Cassandra::Cassandra;
use Cassandra::Pool;
use strict;
use warnings;
### Thrift Protocol/Client methods ###

sub _build_pool {
	my $self = shift;
	#print "BBBBB\n";
	return new Cassandra::Pool(
						 $self->keyspace,
						 {
							server_name => $self->server_name,
							server_port => $self->server_port,
							username    => $self->username,
							password    => $self->password
						 }
	);
}


sub _consistency_level_read {
	my $self = shift;
	my $opt = shift // {};

	my $level = $opt->{consistency_level_read} // $self->consistency_level_read;

	eval "\$level = Cassandra::ConsistencyLevel::$level;";
	$level;
}

sub _consistency_level_write {
	my $self = shift;
	my $opt = shift // {};

	my $level = $opt->{consistency_level_write}
	  // $self->consistency_level_write;

	eval "\$level = Cassandra::ConsistencyLevel::$level;";
	$level;
}

sub _column_or_supercolumn_to_hash {
	my $self = shift;

	my $c_or_sc = shift;

	my @result;
	if ( exists $c_or_sc->{column} and $c_or_sc->{column} ) {
		@result = ( $_->{column}->{name}, $_->{column}->{value} );
	} elsif ( exists $c_or_sc->{super_column} ) {
		@result = (
					$c_or_sc->{super_column}->{name},
					{
					   map { $_->{name} => $_->{value} }
						 @{ $c_or_sc->{super_column}->{columns} }
					}
		);
	}
	return \@result;

}

#### API methods ####

=head2 get

Usage: C<get($column_family, $key[, opt])>
		
C<$opt> is a I<HASH> and can have the following keys:

=over 2

columns, column_start, column_finish, column_count, column_reversed, super_column, consistency_level_read

=back

Returns an HASH of the form C<< { column => value, column => value } >>
	
=cut

sub get {
	my $self = shift;

	my $column_family = shift;
	my $key           = shift;
	my $opt           = shift // {};

	my $columnParent =
	  Cassandra::ColumnParent->new(
							   {
								 column_family => $column_family,
								 super_column  => $opt->{super_column} // undef,
							   }
	  );
	my $predicate = Cassandra::SlicePredicate->new;

	#Cases
	#1 - columns -> getslice with slicepredicate only
	#2 - column_start and end -> getslice with slicerange

	if ( exists $opt->{columns} )
	{ #TODO extra case for when only 1 column is requested, use thrift api's get
		$predicate->{column_names} = $opt->{columns};
	} else {
		my $sliceRange = Cassandra::SliceRange->new($opt);
		$sliceRange->{start}    = $opt->{column_start}    // '';
		$sliceRange->{finish}   = $opt->{column_finish}   // '';
		$sliceRange->{reversed} = $opt->{column_reversed} // 0;
		$sliceRange->{count}    = $opt->{column_count}    // 100;
		$predicate->{slice_range} = $sliceRange;
	}
	my $level = $self->_consistency_level_read($opt);

	my $result =
	  $self->pool->get()->get_slice( $key, $columnParent, $predicate, $level );

	my %result_columns =
	  map {
		my $a = $self->_column_or_supercolumn_to_hash($_);
		$a->[0] => $a->[1]
	  } @{$result};

	return \%result_columns;
}

=head2 multiget

Usage: C<< multiget($column_family, $keys[, opt]) >>
	
C<$keys> should be an I<ARRAYREF> of keys to fetch.
	
All parameters in C<$opt> are the same as in C<get()>
	
Returns an HASH of the form C<< { key => { column => value, column => value }, key => { column => value, column => value } } >>

=cut

sub multiget {
	my $self = shift;

	my $column_family = shift;
	my $keys          = shift;
	my $opt           = shift // {};

	my $columnParent =
	  Cassandra::ColumnParent->new( { column_family => $column_family } );

	my $predicate = Cassandra::SlicePredicate->new;

	#Cases
	#1 - columns -> getslice with slicepredicate only
	#2 - column_start and end -> getslice with slicerange

	if ( exists $opt->{columns} ) {
		$predicate->{column_names} = $opt->{columns};
	} else {
		my $sliceRange = Cassandra::SliceRange->new($opt);
		$sliceRange->{start}    = $opt->{column_start}    // '';
		$sliceRange->{finish}   = $opt->{column_finish}   // '';
		$sliceRange->{reversed} = $opt->{column_reversed} // 0;
		$sliceRange->{count}    = $opt->{column_count}    // 100;
		$predicate->{slice_range} = $sliceRange;
	}
	my $level = $self->_consistency_level_read($opt);

	my $result =
	  $self->pool->get()->multiget_slice( $keys, $columnParent, $predicate, $level );

	my %result_columns = map {
		$_ => { map { $_->{column}->{name} => $_->{column}->{value} }
				@{ $result->{$_} } }
	} keys %$result;

	return \%result_columns;
}

=head2 get_count

Usage: C<< get_count($column_family, $key[, opt]) >>
	
C<$opt> is a I<HASH> and can have the following keys:

=over 2

columns, column_start, column_finish, super_column, consistency_level_read

=back

Returns the count as an int

=cut

sub get_count {

	my $self = shift;

	my $column_family = shift;
	my $key           = shift;
	my $opt           = shift // {};

	my $columnParent =
	  Cassandra::ColumnParent->new(
							   {
								 column_family => $column_family,
								 super_column  => $opt->{super_column} // undef,
							   }
	  );

	my $predicate = Cassandra::SlicePredicate->new;

	#Cases
	#1 - columns -> getslice with slicepredicate only
	#2 - column_start and end -> getslice with slicerange

	if ( exists $opt->{columns} ) {
		$predicate->{column_names} = $opt->{columns};
	} else {
		my $sliceRange = Cassandra::SliceRange->new($opt);
		$sliceRange->{start}    = $opt->{column_start}    // '';
		$sliceRange->{finish}   = $opt->{column_finish}   // '';
		$sliceRange->{reversed} = $opt->{column_reversed} // 0;
		$sliceRange->{count}    = $opt->{column_count}    // 100;
		$predicate->{slice_range} = $sliceRange;
	}
	my $level = $self->_consistency_level_read($opt);

	return $self->pool->get()->get_count( $key, $columnParent, $predicate, $level );
}

=head2 multiget_count

Usage: C<< multiget_count($column_family, $keys[, opt]) >>

C<$keys> should be an I<ARRAYREF> of keys.
	
All parameters in C<$opt> are the same as in C<get_count()>
	
Returns a mapping of C<< key -> count >>

=cut

sub multiget_count {
	my $self = shift;

	my $column_family = shift;
	my $keys          = shift;
	my $opt           = shift // {};

	my $columnParent =
	  Cassandra::ColumnParent->new( { column_family => $column_family } );

	my $predicate = Cassandra::SlicePredicate->new;

	#Cases
	#1 - columns -> getslice with slicepredicate only
	#2 - column_start and end -> getslice with slicerange

	if ( exists $opt->{columns} ) {
		$predicate->{column_names} = $opt->{columns};
	} else {
		my $sliceRange = Cassandra::SliceRange->new($opt);
		$sliceRange->{start}    = $opt->{column_start}    // '';
		$sliceRange->{finish}   = $opt->{column_finish}   // '';
		$sliceRange->{reversed} = $opt->{column_reversed} // 0;
		$sliceRange->{count}    = $opt->{column_count}    // 100;
		$predicate->{slice_range} = $sliceRange;
	}
	my $level = $self->_consistency_level_read($opt);

	return $self->pool->get()->multiget_count( $keys, $columnParent, $predicate,
										  $level );
}

=head2 get_range

Usage: C<get_range( $column_family[, opt])>
	
C<$opt> is a I<HASH> and can have the following keys:

=over 2

start, finish, columns, column_start, column_finish, column_reversed, column_count, row_count, super_column, consistency_level_read

=back
	
Returns an I<HASH> of the form C<< { key => { column => value, column => value }, key => { column => value, column => value } } >>

=cut

sub get_range {
	my $self = shift;

	my $column_family = shift;
	my $opt           = shift;
	my $columnParent =
	  Cassandra::ColumnParent->new(
							   {
								 column_family => $column_family,
								 super_column  => $opt->{super_column} // undef,
							   }
	  );

	my $predicate = Cassandra::SlicePredicate->new;

	#Cases
	#1 - columns -> getslice with slicepredicate only
	#2 - column_start and end -> getslice with slicerange

	if ( exists $opt->{columns} ) {
		$predicate->{column_names} = $opt->{columns};
	} else {
		my $sliceRange = Cassandra::SliceRange->new($opt);
		$sliceRange->{start}    = $opt->{column_start}    // '';
		$sliceRange->{finish}   = $opt->{column_finish}   // '';
		$sliceRange->{reversed} = $opt->{column_reversed} // 0;
		$sliceRange->{count}    = $opt->{column_count}    // 100;
		$predicate->{slice_range} = $sliceRange;
	}

	my $keyRange =
	  Cassandra::KeyRange->new(
								{
								  start_key => $opt->{start}     // '',
								  end_key   => $opt->{finish}    // '',
								  count     => $opt->{row_count} // 100,
								}
	  );

	my $level = $self->_consistency_level_read($opt);

	my $result =
	  $self->pool->get()->get_range_slices( $columnParent, $predicate,
									   $keyRange,     $level );

	my %result_columns = map {
		$_->{key} => {
			map {
				my $a = $self->_column_or_supercolumn_to_hash($_);
				$a->[0] => $a->[1]
			  } @{ $_->{columns} }
		  }
	} @{$result};

	return \%result_columns;
}

=head2 get_indexed_slices

Usage: C<get_indexed_slices($column_family, $index_clause[, opt])>
	
C<$index_clause> is an I<HASH> containing the following keys:

=over 2

expression_list, start_key, row_count

The I<expression_list> is an I<ARRAYREF> of the form C<< [ [ column => value ] ] >>

=back	

C<$opt> is an I<HASH> and can have the following keys:

=over 2

columns, column_start, column_finish, column_reversed, column_count, consistency_level_read

=back

Returns an I<HASH> of the form C<< { key => { column => value, column => value }, key => { column => value, column => value } } >>
	
=cut

sub get_indexed_slices {
	my $self = shift;

	my $column_family = shift;
	my $index_clause  = shift;
	my $opt           = shift // {};

	my $expr_list;
	my $predicate_args;

	my $columnParent =
	  Cassandra::ColumnParent->new( { column_family => $column_family } );

	my @index_expr = map {
		Cassandra::IndexExpression->new(
										 {
										   column_name => $_->[0],
										   op => Cassandra::IndexOperator::EQ,
										   value => $_->[1]
										 }
		);
	} @{ $index_clause->{'expression_list'} };

	my $index_clause_thrift =
	  Cassandra::IndexClause->new(
							   {
								 expressions => \@index_expr,
								 start_key => $index_clause->{start_key} // '',
								 count => $index_clause->{row_count} // 100,
							   }
	  );

	my $predicate = Cassandra::SlicePredicate->new;

	#Cases
	#1 - columns -> getslice with slicepredicate only
	#2 - column_start and end -> getslice with slicerange

	if ( exists $opt->{columns} ) {
		$predicate->{column_names} = $opt->{columns};
	} else {
		my $sliceRange = Cassandra::SliceRange->new($opt);
		$sliceRange->{start}    = $opt->{column_start}    // '';
		$sliceRange->{finish}   = $opt->{column_finish}   // '';
		$sliceRange->{reversed} = $opt->{column_reversed} // 0;
		$sliceRange->{count}    = $opt->{column_count}    // 100;
		$predicate->{slice_range} = $sliceRange;
	}

	my $level = $self->_consistency_level_read($opt);

	my $result =
	  $self->pool->get()->get_indexed_slices( $columnParent, $index_clause_thrift,
										 $predicate, $level );

	my %result_keys = map {
		$_->{key} => { map { $_->{column}->{name} => $_->{column}->{value} }
					   @{ $_->{columns} } }
	} @{$result};

	return \%result_keys;
}

=head2 insert

Usage: C<< insert($column_family, $key, $columns[, opt]) >>
	
The C<$columns> is an I<HASHREF> of the form C<< { column => value, column => value } >>
	
C<$opt> is an I<HASH> and can have the following keys:

=over 2

timestamp, ttl, consistency_level_write

=back

=cut

sub insert {
	my $self = shift;

	my $column_family = shift;
	my $key           = shift;
	my $columns       = shift;
	my $opt           = shift // {};

	my $columnParent =
	  Cassandra::ColumnParent->new( { column_family => $column_family } );
	my $level = $self->_consistency_level_write($opt);

	my @mutations = map {
		new Cassandra::Mutation(
						{
						  column_or_supercolumn =>
							Cassandra::ColumnOrSuperColumn->new(
							   {
								 column =>
								   new Cassandra::Column(
									  {
										name      => $_,
										value     => $columns->{$_},
										timestamp => $opt->{timestamp} // time,
										ttl       => $opt->{ttl} // undef,
									  }
								   )
							   }
							)
						}
		  )
	} keys %$columns;

	$self->pool->get()->batch_mutate( { $key => { $column_family => \@mutations } },
								 $level );
}

=head2 insert_super

Usage: C<< insert_super($column_family, $key, $columns[, opt]) >>
	
The C<$columns> is an I<HASH> of the form C<< { super_column => { column => value, column => value } } >>
	
C<$opt> is an I<HASH> and can have the following keys:

=over 2

timestamp, ttl, consistency_level_write

=back

=cut

sub insert_super {
	my $self = shift;

	my $column_family = shift;
	my $key           = shift;
	my $columns       = shift;
	my $opt           = shift // {};

	my $columnParent =
	  Cassandra::ColumnParent->new( { column_family => $column_family } );
	my $level = $self->_consistency_level_write($opt);

	my @mutations = map {
		my $arg = $_;
		new Cassandra::Mutation(
			{
			   column_or_supercolumn => Cassandra::ColumnOrSuperColumn->new(
				   {
					  super_column => Cassandra::SuperColumn->new(
						  {
							 name    => $_,
							 columns => [
								 map {
									 new Cassandra::Column(
											 {
											   name  => $_,
											   value => $columns->{$arg}->{$_},
											   timestamp => $opt->{timestamp}
												 // time,
											   ttl => $opt->{ttl} // undef,
											 }
									   )
								   } keys %{ $columns->{$arg} }
							 ],

						  }
					  )
				   }
			   )
			}
		  )
	} keys %$columns;

	$self->pool->get()->batch_mutate( { $key => { $column_family => \@mutations } },
								 $level );
}

=head2 batch_insert

Usage: C<batch_insert($column_family, $rows[, opt])>

C<$rows> is an I<HASH> of the form C<< { key => { column => value , column => value }, key => { column => value , column => value } } >>
	
C<$opt> is an I<HASH> and can have the following keys:

=over 2

timestamp, ttl, consistency_level_write

=back

=cut

sub batch_insert {
	my $self = shift;

	my $column_family = shift;
	my $rows          = shift;
	my $opt           = shift // {};

	my $columnParent =
	  Cassandra::ColumnParent->new( { column_family => $column_family } );
	my $level = $self->_consistency_level_write($opt);

	my %mutation_map = map {
		my $columns = $rows->{$_};
		$_ => {
			$column_family => [
				map {
					new Cassandra::Mutation(
						{
						   column_or_supercolumn =>
							 Cassandra::ColumnOrSuperColumn->new(
							   {
								  column =>
									new Cassandra::Column(
									  {
										 name      => $_,
										 value     => $columns->{$_},
										 timestamp => $opt->{timestamp} // time,
										 ttl       => $opt->{ttl} // undef,
									  }
									)
							   }
							 )
						}
					);
				  } keys %$columns
			]
		  }
	} keys %$rows;

	$self->pool->get()->batch_mutate( \%mutation_map, $level );
}

=head2 remove

Usage: C<< remove($column_family[, $keys][, opt]) >>
	
C<$keys> is a key or an I<ARRAY> of keys to be deleted.

A removal whitout keys truncates the whole column_family.
	
C<$opt> is an I<HASH> and can have the following keys:

=over 2

columns, super_column, write_consistency_level

=back

The timestamp used for remove is returned.

=cut

sub remove {
	my $self = shift;

	my $column_family = shift;
	my $keys          = shift;
	my $opt           = shift // {};

	my $timestamp = time;
	my $level     = $self->_consistency_level_write($opt);

	if ($keys) {

		$keys = [$keys] unless ref($keys) eq 'ARRAY';

		my $deletion =
		  Cassandra::Deletion->new(
							   {
								 timestamp    => $timestamp,
								 super_column => $opt->{super_column} // undef,
							   }
		  );

		if ( exists $opt->{columns} ) {
			$deletion->{predicate} = Cassandra::SlicePredicate->new(
										  { column_names => $opt->{columns} } );
		}

		#		else {#Unsupported by Cassandra yet
		#			my $predicate = Cassandra::SlicePredicate->new;
		#			my $sliceRange = Cassandra::SliceRange->new($opt);
		#			$sliceRange->{start}    = $opt->{column_start}    // '';
		#			$sliceRange->{finish}   = $opt->{column_finish}   // '';
		#			$sliceRange->{reversed} = $opt->{column_reversed} // 0;
		#			$sliceRange->{count}    = $opt->{column_count}    // 100;
		#			$predicate->{slice_range} = $sliceRange;
		#			$deletion->{predicate} = $predicate;
		#		}

		my %mutation_map = map {
			$_ => { $column_family =>
					[ new Cassandra::Mutation( { deletion => $deletion, } ) ] }
		} @{$keys};

		$self->pool->get()->batch_mutate( \%mutation_map, $level );

		return $timestamp;
	} else {
		$self->pool->get()->truncate($column_family);
	}
}

=head2 list_keyspace_cfs

Usage: C<< list_keyspace_cfs($keyspace) >>

Returns an HASH of C<< { column_family_name => column_family_type } >> where column family type is either C<Standard> or C<Super> 

=cut

sub list_keyspace_cfs {
	my ( $self, $keyspace ) = @_;
	my $result = $self->pool->get()->describe_keyspace($keyspace);

	return map { $_->{name} => $_->{column_type} } @{ $result->{cf_defs} };
}

=head2 create_column_family

Usage C<< create_column_family($keyspace, $column_family[, $is_super][, $comment]) >>

C<$is_super> is a boolean indicating if this is a Standard or Super Column Family.

=cut

sub create_column_family {
	my $self = shift;

	my $keyspace      = shift;
	my $column_family = shift;
	my $is_super      = shift // 0;
	my $comment       = shift // undef;

	my $cfdef = Cassandra::CfDef->new();

	$cfdef->{name}        = $column_family;
	$cfdef->{keyspace}    = $keyspace;
	$cfdef->{comment}     = $comment;
	$cfdef->{column_type} = $is_super ? 'Super' : 'Standard';

	$self->pool->get()->system_add_column_family($cfdef);
}

=head2 create_index

Usage: C<< create_index($keyspace, $column_family, $column, [$validation_class]) >>

Creates an index on C<$column> of C<$column_family>.

C<$validation_class> only applies when C<$column> doesn't yet exist, and even then it is optional (defaults to I<BytesType>).

=cut

sub create_index {
	my $self = shift;

	my $keyspace      = shift;
	my $column_family = shift;
	my $column        = shift;

#get column family definition, substitute the target column with itself but indexed.

	my $cfdef =
	  [ grep { $_->{name} eq $column_family }
		@{ $self->pool->get()->describe_keyspace($keyspace)->{cf_defs} } ]->[0];

	my $cdef;
	if ( @{ $cfdef->{column_metadata} }
		 and grep { $_->{name} eq $column } @{ $cfdef->{column_metadata} } )
	{
		$cdef =
		  [ grep { $_->{name} eq $column } @{ $cfdef->{column_metadata} } ]
		  ->[0];
	} else {
		$cdef = new Cassandra::ColumnDef(
			 {
			   name             => $column,
			   validation_class => 'org.apache.cassandra.db.marshal.BytesType',
			 }
		);
	}
	$cdef->{index_type} = 0;
	$cdef->{index_name} = $column . "_idx";

	$cfdef->{column_metadata} =
	  [ grep { $_->{name} ne $column } @{ $cfdef->{column_metadata} } ];
	push @{ $cfdef->{column_metadata} }, $cdef;

	#print Dumper $cfdef;

	$self->pool->get()->system_update_column_family($cfdef);
}

=head2 ring

Usage: C<< ring($keyspace) >>

Lists the addresses of all nodes on the cluster associated with the keyspace C<<$keyspace>>.

=cut
sub ring {

	my $self = shift;

	my $keyspace = shift;

	return
	  map { $_->{endpoints}->[0] } @{ $self->pool->get()->describe_ring($keyspace) };
}

=head1 BUGS

Bugs should be reported on github at L<https://github.com/fmgoncalves/p5-cassandra-simple>.

=cut

#TODO TODOs

=head1 TODO

B<Unit Tests>

=over 2

Sort of done in the examples folder
L<https://github.com/fmgoncalves/p5-cassandra-simple/tree/master/examples>

=back

B<Tombstones>

get, get_range and get_indexed_slices should probably filter out tombstones, even if it means returning less than the requested count.
Ideally it would retry until it got enough results.

B<Methods>

The following are Thrift methods left unimplemented. 

Not all of these will be implemented, since some aren't useful to the common developer. 

Priority will be given to live schema updating methods.

=over 2

describe_cluster_name
	
string describe_cluster_name()
	
describe_keyspace
	
KsDef describe_keyspace(string keyspace)
	
describe_keyspaces
	
list<KsDef> describe_keyspaces()
	
describe_partitioner
	
string describe_partitioner()
	
describe_snitch
	
string describe_snitch()
	
describe_version
	
string describe_version()
	
system_drop_column_family
	
string system_drop_column_family(ColumnFamily column_family)
	
system_add_keyspace
	
string system_add_keyspace(KSDef ks_def)
	
system_drop_keyspace
	
string system_drop_keyspace(string keyspace)

=back


=cut

=head1 ACKNOWLEDGEMENTS

Implementation based on Cassandra::Lite.

=over 2

L<http://search.cpan.org/~gslin/Cassandra-Lite-0.0.4/lib/Cassandra/Lite.pm>

=back

API based on Pycassa.

=over 2

L<http://pycassa.github.com/pycassa/>

=back

=cut

=head1 AUTHOR

Filipe Gonçalves C<< <the.wa.syndrome@gmail> >>

=cut

=head1 COPYRIGHT AND LICENSE

This software is copyright (c) 2011 by Filipe Gonçalves.

This is free software; you can redistribute it and/or modify it under the same terms as the Perl 5 programming language system itself.

=cut

1;
