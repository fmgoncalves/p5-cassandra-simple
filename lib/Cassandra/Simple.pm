package Cassandra::Simple;

=pod

=encoding utf8

=head1 NAME

Cassandra::Simple

=head1 VERSION

version 0.2

=head1 DESCRIPTION

Easy to use, Perl oriented client interface to Apache Cassandra.

This module attempts to abstract the underlying Thrift methods as much as possible to allow any Perl developer a small learning curve when using Cassandra.


=head1 SYNOPSYS

	my ($keyspace, $column_family) = qw/simple simple/;

	my $conn = Cassandra::Simple->new(keyspace => $keyspace,);

	$conn->create_column_family( column_family => $column_family);

	$conn->insert(column_family => $column_family, key => 'KeyA', columns => { 'ColumnA' => 'AA' , 'ColumnB' => 'AB' } );

	$conn->get(column_family => $column_family, key => 'KeyA');
	$conn->get(column_family => $column_family, key => 'KeyA', columns => [ qw/ColumnA/ ]);
	$conn->get(column_family => $column_family, key => 'KeyA', column_count => 1, column_reversed => 1);

	$conn->batch_insert(column_family => $column_family, rows => { 'KeyB' => [ [ 'ColumnA' => 'BA' ] , [ 'ColumnB' => 'BB' ] ], 'KeyC' => [ [ 'ColumnA' => 'CA' ] , [ 'ColumnD' => 'CD' ] ] });

	$conn->multiget(column_family => $column_family, 'keys' => [qw/KeyA KeyC/]);

	$conn->get_range(column_family => $column_family, start => 'KeyA', finish => 'KeyB', column_count => 1 );
	$conn->get_range(column_family => $column_family);

	$conn->get_indexed_slices(column_family => $column_family, expression_list => [ [ 'ColumnA' => 'BA' ] ]);

	$conn->remove(column_family => $column_family, 'keys' => [ 'KeyA' ], columns => [ 'ColumnA' ]);
	$conn->remove(column_family => $column_family, 'keys' => [ 'KeyA' ]);
	$conn->remove(column_family => $column_family);

	$conn->get_count(column_family => $column_family, key => 'KeyA');
	$conn->multiget_count(column_family => $column_family, 'keys' => [ 'KeyB', 'KeyC' ]);


=cut


use strict;
use warnings;

our $VERSION = "0.2";

use Data::Dumper;

use Any::Moose;
has 'pool' => ( is => 'rw', isa => 'Cassandra::Pool', lazy_build => 1 );
has 'consistency_level_read' =>
  ( is => 'rw', isa => 'Str', default => 'LOCAL_QUORUM' );
has 'consistency_level_write' => ( is => 'rw', isa => 'Str', default => 'ONE' );
has 'keyspace'    => ( is => 'rw', isa => 'Str' );
has 'password'    => ( is => 'rw', isa => 'Str', default => '' );
has 'server_name' => ( is => 'rw', isa => 'Str', default => 'localhost' );
has 'server_port' => ( is => 'rw', isa => 'Int', default => 9160 );

has 'username' => ( is => 'rw', isa => 'Str', default => '' );
has 'pool_from_ring' => ( is => 'rw', isa =>  'Any', trigger => sub { my $self = shift; $self->pool->add_pool_from_ring() if shift });

use 5.010;
use Cassandra::Cassandra;
use Cassandra::Pool;
use Cassandra::LazyQuery;
use strict;
use warnings;
use Time::HiRes qw/gettimeofday/;
use Tie::IxHash;
use Try::Tiny;
use Switch;
### Thrift Protocol/Client methods ###

sub _build_pool {
	my ($self) = @_;

	return
	  Cassandra::Pool->new(
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

	my $level = $opt->{consistency_level} // $self->consistency_level_read;

	eval "\$level = Cassandra::ConsistencyLevel::$level;";
	return $level;
}

sub _consistency_level_write {
	my $self = shift;
	my $opt = shift // {};

	my $level = $opt->{consistency_level}
	  // $self->consistency_level_write;

	eval "\$level = Cassandra::ConsistencyLevel::$level;";
	return $level;
}

sub ordered_hash{
	tie my %newhash, 'Tie::IxHash', @_;
	return \%newhash;
}

sub _column_or_supercolumn_to_hash {
	my ($cf, $c_or_sc) = @_;

	my $result;
	if ( exists $c_or_sc->{column} and $c_or_sc->{column} ) {
		$result = ordered_hash( $_->{column}->{name}, $_->{column}->{value} );
	} elsif ( exists $c_or_sc->{super_column} and $c_or_sc->{super_column} ) {
		$result = ordered_hash(
					$c_or_sc->{super_column}->{name},
					ordered_hash(
					   map { $_->{name} => $_->{value} }
						 @{ $c_or_sc->{super_column}->{columns} }
					)
		);
	} elsif ( exists $c_or_sc->{counter_column} and $c_or_sc->{counter_column} )
	{
		$result =
		  ordered_hash( $_->{counter_column}->{name}, $_->{counter_column}->{value} );
	} elsif ( exists $c_or_sc->{counter_super_column}
			  and $c_or_sc->{counter_super_column} )
	{
		$result = ordered_hash(
					$c_or_sc->{counter_super_column}->{name},
					ordered_hash(
					   map { $_->{name} => $_->{value} }
						 @{ $c_or_sc->{counter_super_column}->{columns} }
					)
		);
	}
	return $result;

}

sub _index_operator {
	my $self = shift;
	my $operator = shift || "=";

	return {
			 "="  => Cassandra::IndexOperator::EQ,
			 "<"  => Cassandra::IndexOperator::LT,
			 ">"  => Cassandra::IndexOperator::GT,
			 "<=" => Cassandra::IndexOperator::LTE,
			 ">=" => Cassandra::IndexOperator::GTE,
	}->{$operator};

}

sub _wait_for_agreement {
	my $self = shift;

	while ( 1 != scalar keys % { $self->_run_query('describe_schema_versions') } )
	{
		sleep 0.25;
	}
	
	return 1;
}

sub _run_query {
	my ($self, $method, @params) = @_;
	my $cl    = $self->pool->get();
	
	return try {
		#print "REQUEST:\n\t" .Dumper [ $method, @params ];
		my $result = $cl->$method( @params );
		#print "RESPONSE:\n\t".Dumper $result;
		$self->pool->put($cl);
		return $result;
	} catch {
		switch (ref($_)){
			case [ 'Thrift::TException', 'Cassandra::UnavailableException', 'Cassandra::TimedOutException',  'SchemaDisagreementException' ]
				{ 
					$self->pool->fail($cl);
#					print 'Temporary failure ('.ref($_).'):'.$_->{why}."\n";
				}
			case [ 'TApplicationException', 'Cassandra::InvalidRequestException', 'Cassandra::NotFoundException' ]
				{
					$self->pool->put($cl);
#					print 'Request error or permanent failure ('.ref($_).'):'.$_->{why}."\n";
				}
			else  {
#				print 'Unknown error '.ref($_)."\n";
				$self->pool->put($cl);
			}
		}
		die $_;
	};
}

sub lazy_query {
	my ($self, $method, @params) = @_;
	return Cassandra::LazyQuery->new($self, $method, @params);
}

#### API methods ####

=head2 get

Arguments:

=over 2

column_family, key, columns, column_start, column_finish, column_count, column_reversed, super_column, consistency_level

=back

Returns an HASH of the form C<< { column => value, column => value } >>

=cut

sub get {
	my ($self, @params) = @_;
	my $arguments = {
		@params
	};

	my $columnParent =
	  Cassandra::ColumnParent->new(
							   {
								 column_family => $arguments->{column_family},
								 super_column  => $arguments->{super_column},
							   }
	  );
	my $predicate = Cassandra::SlicePredicate->new;

	#Cases
	#1 - columns -> getslice with slicepredicate only
	#2 - column_start and end -> getslice with slicerange

	if ( exists $arguments->{columns} )
	{ #TODO extra case for when only 1 column is requested, use thrift api's get
		$predicate->{column_names} = $arguments->{columns};
	} else {
		my $sliceRange = Cassandra::SliceRange->new();
		$sliceRange->{start}    = $arguments->{column_start}    // '';
		$sliceRange->{finish}   = $arguments->{column_finish}   // '';
		$sliceRange->{reversed} = $arguments->{column_reversed} // 0;
		$sliceRange->{count}    = $arguments->{column_count}    // 100;
		$predicate->{slice_range} = $sliceRange;
	}
	
	my $level = $self->_consistency_level_read($arguments);

	my $result = $self->_run_query('get_slice', $arguments->{key}, $columnParent, $predicate, $level ) ;
		
	my $result_columns = ordered_hash();
	foreach(@$result) {
		my $a = &_column_or_supercolumn_to_hash( $arguments->{column_family}, $_ );
		@$result_columns{keys %$a} = values %$a;
	 };
	return $result_columns;
	
}

=head2 multiget

Arguments:

=over 2

column_family, keys, columns, column_start, column_finish, column_count, column_reversed, super_column, consistency_level

=back

Returns an HASH of the form C<< { key => { column => value, column => value }, key => { column => value, column => value } } >>

=cut

sub multiget {
	my ($self, @params) = @_;
	my $arguments = {
		@params
	};
	
	my $columnParent =
	  Cassandra::ColumnParent->new( { column_family => $arguments->{column_family} } );

	my $predicate = Cassandra::SlicePredicate->new;

	#Cases
	#1 - columns -> getslice with slicepredicate only
	#2 - column_start and end -> getslice with slicerange

	if ( exists $arguments->{columns} ) {
		$predicate->{column_names} = $arguments->{columns};
	} else {
		my $sliceRange = Cassandra::SliceRange->new();
		$sliceRange->{start}    = $arguments->{column_start}    // '';
		$sliceRange->{finish}   = $arguments->{column_finish}   // '';
		$sliceRange->{reversed} = $arguments->{column_reversed} // 0;
		$sliceRange->{count}    = $arguments->{column_count}    // 100;
		$predicate->{slice_range} = $sliceRange;
	}
	my $level = $self->_consistency_level_read($arguments);
	my @match = @{$arguments->{keys}};
        my $result;
	#if you don't have keys don't allow run query
        if(scalar @match != 0){
                $result =  $self->_run_query('multiget_slice', $arguments->{keys}, $columnParent, $predicate, $level );
        }else{
                return $result;
        }	
		
	my $result_columns = ordered_hash();
	foreach my $key (@{$arguments->{keys}}) {
		if ($result->{$key}){
			$result_columns->{$key} = ordered_hash();
			foreach(@{$result->{$key}}){
				my $a = &_column_or_supercolumn_to_hash( $arguments->{column_family}, $_ );
				@{$result_columns->{$key}}{keys %$a} = values %$a;
			}
		}
	 };

	return $result_columns;
}

=head2 get_count

Arguments:

=over 2

column_family, key, columns, column_start, column_finish, super_column, consistency_level

=back

Returns the count as an int

=cut

sub get_count {
	my ($self, @params) = @_;
	my $arguments = {
		@params
	};

	my $columnParent =
	  Cassandra::ColumnParent->new(
							   {
								 column_family => $arguments->{column_family},
								 super_column  => $arguments->{super_column},
							   }
	  );

	my $predicate = Cassandra::SlicePredicate->new;

	#Cases
	#1 - columns -> getslice with slicepredicate only
	#2 - column_start and end -> getslice with slicerange

	if ( exists $arguments->{columns} ) {
		$predicate->{column_names} = $arguments->{columns};
	} else {
		my $sliceRange = Cassandra::SliceRange->new();
		$sliceRange->{start}    = $arguments->{column_start}    // '';
		$sliceRange->{finish}   = $arguments->{column_finish}   // '';
		$sliceRange->{reversed} = $arguments->{column_reversed} // 0;
		$sliceRange->{count}    = $arguments->{column_count}    // 100;
		$predicate->{slice_range} = $sliceRange;
	}
	my $level = $self->_consistency_level_read($arguments);

	my $result =  $self->_run_query( 'get_count', $arguments->{key}, $columnParent, $predicate, $level ) ;
	
	return $result;
	
}

=head2 multiget_count

Arguments:

=over 2

column_family, keys, columns, column_start, column_finish, super_column, consistency_level

=back

Returns a mapping of C<< key -> count >>

=cut

sub multiget_count {
	my ($self, @params) = @_;
	my $arguments = {
		@params
	};
	
	my $columnParent =
	  Cassandra::ColumnParent->new( { column_family => $arguments->{column_family} } );

	my $predicate = Cassandra::SlicePredicate->new;

	#Cases
	#1 - columns -> getslice with slicepredicate only
	#2 - column_start and end -> getslice with slicerange

	if ( exists $arguments->{columns} ) {
		$predicate->{column_names} = $arguments->{columns};
	} else {
		my $sliceRange = Cassandra::SliceRange->new();
		$sliceRange->{start}    = $arguments->{column_start}    // '';
		$sliceRange->{finish}   = $arguments->{column_finish}   // '';
		$sliceRange->{reversed} = $arguments->{column_reversed} // 0;
		$sliceRange->{count}    = $arguments->{column_count}    // 100;
		$predicate->{slice_range} = $sliceRange;
	}
	my $level = $self->_consistency_level_read($arguments);
	
	my $result =  $self->_run_query( 'multiget_count', $arguments->{'keys'}, $columnParent, $predicate, $level ) ;
	
	return $result;
}

=head2 get_range

Arguments:

=over 2

column_family, start, finish, columns, column_start, column_finish, column_reversed, column_count, row_count, super_column, consistency_level

=back

Returns an I<HASH> of the form C<< { key => { column => value, column => value }, key => { column => value, column => value } } >>

=cut

sub get_range {
	my ($self, @params) = @_;
	my $arguments = {
		@params
	};
	
	my $columnParent =
	  Cassandra::ColumnParent->new(
							   {
								 column_family => $arguments->{column_family},
								 super_column  => $arguments->{super_column},
							   }
	  );

	my $predicate = Cassandra::SlicePredicate->new;

	#Cases
	#1 - columns -> getslice with slicepredicate only
	#2 - column_start and end -> getslice with slicerange

	if ( exists $arguments->{columns} ) {
		$predicate->{column_names} = $arguments->{columns};
	} else {
		my $sliceRange = Cassandra::SliceRange->new();
		$sliceRange->{start}    = $arguments->{column_start}    // '';
		$sliceRange->{finish}   = $arguments->{column_finish}   // '';
		$sliceRange->{reversed} = $arguments->{column_reversed} // 0;
		$sliceRange->{count}    = $arguments->{column_count}    // 100;
		$predicate->{slice_range} = $sliceRange;
	}

	my $keyRange =
	  Cassandra::KeyRange->new(
								{
								  start_key => $arguments->{start}     // '',
								  end_key   => $arguments->{finish}    // '',
								  count     => $arguments->{row_count} // 100,
								}
	  );

	my $level  = $self->_consistency_level_read($arguments);
	
	my $result =  $self->_run_query('get_range_slices', $columnParent, $predicate, $keyRange, $level );
	
	my $result_columns = ordered_hash();
	foreach my $row(@$result) {
		$result_columns->{$row->{key}} = ordered_hash();
		foreach(@{$row->{columns}}){
			my $a = &_column_or_supercolumn_to_hash( $arguments->{column_family}, $_ );
			@{$result_columns->{$row->{key}}}{keys %$a} = values %$a;
		}
	};
			
	return $result_columns;
}

=head2 get_indexed_slices

Arguments:

=over 2

column_family, expression_list, start, row_count, columns, column_start, column_finish, column_reversed, column_count, consistency_level

=back

The I<expression_list> is an I<ARRAYREF> of I<ARRAYREF> containing C<<  $column[, $operator], $value >>. C<$operator> can be '=', '<', '>', '<=' or '>='.

Returns an I<HASH> of the form C<< { key => { column => value, column => value }, key => { column => value, column => value } } >>

=cut

sub get_indexed_slices {
	my ($self, @params) = @_;
	my $arguments = {
		@params
	};
	
	my $expr_list;
	my $predicate_args;

	my $columnParent =
	  Cassandra::ColumnParent->new( { column_family => $arguments->{column_family} } );

	my @index_expr = map {
		my ( $col, $op, $val ) = @$_;
		( $op, $val ) = ( $val, $op ) unless $val;
		Cassandra::IndexExpression->new(
										 {
										   column_name => $col,
										   op    => $self->_index_operator($op),
										   value => $val
										 }
		);
	} @{ $arguments->{'expression_list'} };

	my $index_clause_thrift =
	  Cassandra::IndexClause->new(
							   {
								 expressions => \@index_expr,
								 start_key => $arguments->{start} // '',
								 count => $arguments->{row_count} // 100,
							   }
	  );

	my $predicate = Cassandra::SlicePredicate->new();

	#Cases
	#1 - columns -> getslice with slicepredicate only
	#2 - column_start and end -> getslice with slicerange

	if ( exists $arguments->{columns} ) {
		$predicate->{column_names} = $arguments->{columns};
	} else {
		my $sliceRange = Cassandra::SliceRange->new();
		$sliceRange->{start}    = $arguments->{column_start}    // '';
		$sliceRange->{finish}   = $arguments->{column_finish}   // '';
		$sliceRange->{reversed} = $arguments->{column_reversed} // 0;
		$sliceRange->{count}    = $arguments->{column_count}    // 100;
		$predicate->{slice_range} = $sliceRange;
	}

	my $level = $self->_consistency_level_read($arguments);

	my $result =  $self->_run_query('get_indexed_slices', $columnParent, $index_clause_thrift,
								 $predicate, $level );

	my $result_columns = ordered_hash();
	foreach my $row(@$result) {
		$result_columns->{$row->{key}} = ordered_hash();
		foreach(@{$row->{columns}}){
			my $a = &_column_or_supercolumn_to_hash( $arguments->{column_family}, $_ );
			@{$result_columns->{$row->{key}}}{keys %$a} = values %$a;
		}
	 };

	return $result_columns;
}

=head2 insert

Arguments:

=over 2

column_family, key, columns, timestamp, ttl, consistency_level

=back

The C<$columns> is an I<HASHREF> of the form C<< { column => value, column => value } >>

=cut

sub insert {
	my ($self, @params) = @_;
	my $arguments = {
		timestamp => int (gettimeofday * 1000000),
		@params
	};

	my $columnParent =
	  Cassandra::ColumnParent->new( { column_family => $arguments->{column_family} } );
	my $level = $self->_consistency_level_write($arguments);
	my @mutations = map {
		Cassandra::Mutation->new(
						{
						  column_or_supercolumn =>
							Cassandra::ColumnOrSuperColumn->new(
							   {
								 column =>
								   Cassandra::Column->new(
									  {
										name      => $_,
										value     => $arguments->{columns}->{$_},
										timestamp => $arguments->{timestamp},
										ttl       => $arguments->{ttl},
									  }
								   )
							   }
							)
						}
		  )
	} keys % { $arguments->{columns} };
	
	$self->_run_query('batch_mutate', { $arguments->{key} => { $arguments->{column_family} => \@mutations } }, $level );
	return $arguments->{timestamp};
}

=head2 insert_super

Arguments:

=over 2

column_family, key, columns, timestamp, ttl, consistency_level

=back

The C<$columns> is an I<HASH> of the form C<< { super_column => { column => value, column => value } } >>

=cut

sub insert_super {
	my ($self, @params) = @_;
	my $arguments = {
		timestamp => int (gettimeofday * 1000000),
		@params
	};

	my $columnParent =
	  Cassandra::ColumnParent->new( { column_family => $arguments->{column_family} } );
	my $level = $self->_consistency_level_write($arguments);

	my @mutations = map {
		my $arg = $_;
		Cassandra::Mutation->new(
			{
			   column_or_supercolumn => Cassandra::ColumnOrSuperColumn->new(
				   {
					  super_column => Cassandra::SuperColumn->new(
						  {
							 name    => $_,
							 columns => [
								 map {
									 Cassandra::Column->new(
											 {
											   name  => $_,
											   value => $arguments->{columns}->{$arg}->{$_},
											   timestamp => $arguments->{timestamp},
											   ttl => $arguments->{ttl},
											 }
									   )
								   } keys % { $arguments->{columns}->{$arg} }
							 ],
						  }
					  )
				   }
			   )
			}
		  )
	} keys % {$arguments->{columns}};

	$self->_run_query( 'batch_mutate', { $arguments->{key} => { $arguments->{column_family} => \@mutations } }, $level );
	return $arguments->{timestamp};
}

=head2 batch_insert

Arguments:

=over 2

column_family, rows, timestamp, ttl, consistency_level

=back

C<$rows> is an I<HASH> of the form C<< { key => { column => value , column => value }, key => { column => value , column => value } } >>

=cut

sub batch_insert {
	my ($self, @params) = @_;
	my $arguments = {
		timestamp => int (gettimeofday * 1000000),
		@params
	};

	my $columnParent =
	  Cassandra::ColumnParent->new( { column_family => $arguments->{column_family} } );
	my $level = $self->_consistency_level_write($arguments);

	my %mutation_map = map {
		my $columns = $arguments->{rows}->{$_};
		$_ => {
			$arguments->{column_family} => [
				map {
					my $column_name = $_;
					Cassandra::Mutation->new(
						{
						   column_or_supercolumn =>
							 Cassandra::ColumnOrSuperColumn->new(
							   {
							   		super_column => UNIVERSAL::isa($columns->{$column_name}, 'HASH') ?
								   		Cassandra::SuperColumn->new({
								   			name => $column_name,
								   			columns => [
									   			map {
									   				Cassandra::Column->new(
													  {
														 name      => $_,
														 value     => $columns->{$column_name}->{$_},
														 timestamp => $arguments->{timestamp},
														 ttl       => $arguments->{ttl},
													  }
													)
									   			} keys %{$columns->{$column_name}} ]
								   		}): undef
							   		,
								  column => UNIVERSAL::isa($columns->{$column_name}, 'HASH') ? undef:
									Cassandra::Column->new(
									  {
										 name      => $_,
										 value     => $columns->{$_},
										 timestamp => $arguments->{timestamp},
										 ttl       => $arguments->{ttl},
									  }
									)
							   }
							 )
						}
					);
				  } keys %$columns
			]
		  }
	} keys % {$arguments->{rows}};
	
	$self->_run_query( 'batch_mutate', \%mutation_map, $level );
	return $arguments->{timestamp};
}

=head2 add

Arguments:

=over 2

column_family, key, column, value, super_column, consistency_level

=back

Increment or decrement counter C<$column> by C<$value>. C<$value> is 1 by default.

=cut

sub add {
	my ($self, @params) = @_;
	my $arguments = {
		value => 1,
		@params
	};

	my $level = $self->_consistency_level_write($arguments);
	my $columnParent =
	  Cassandra::ColumnParent->new(
							   {
								 column_family => $arguments->{column_family},
								 super_column  => $arguments->{super_column},
							   }
	  );
	my $col =
	  Cassandra::CounterColumn->new( { name => $arguments->{column}, value => $arguments->{value} } );

	return $self->_run_query('add', $arguments->{key}, $columnParent, $col, $level );
}

=head2 batch_add

Arguments:

=over 2

column_family, rows, consistency_level

=back

C<$rows> is an I<HASH> of the form C<< { key => { column => value , column => value }, key => { column => value , column => value } } >>

=cut

sub batch_add {
	my ($self, @params) = @_;
	my $arguments = {
		@params
	};

	my $columnParent =
	  Cassandra::ColumnParent->new( { column_family => $arguments->{column_family}, super_column => $arguments->{super_column} } );
	my $level = $self->_consistency_level_write($arguments);

	my %mutation_map = map {
		my $columns = $arguments->{rows}->{$_};
		$_ => {
			$arguments->{column_family} => [
				map {
					my $column_name = $_;
					Cassandra::Mutation->new(
						{
						   column_or_supercolumn =>
							 Cassandra::ColumnOrSuperColumn->new(
							   {
							   		counter_super_column => UNIVERSAL::isa($columns->{$column_name}, 'HASH') ?
								   		Cassandra::CounterSuperColumn->new({
								   			name => $column_name,
								   			columns => [
									   			map {
									   				Cassandra::CounterColumn->new(
													  {
														 name      => $_,
														 value     => $columns->{$column_name}->{$_},
													  }
													)
									   			} keys %{$columns->{$column_name}} ]
								   		}): undef
							   		,
								  counter_column => UNIVERSAL::isa($columns->{$column_name}, 'HASH') ? undef:
									Cassandra::CounterColumn->new(
									  {
										 name      => $_,
										 value     => $columns->{$_},
									  }
									)
							   }
							 )
						}
					);
				  } keys %$columns
			]
		  }
	} keys % {$arguments->{rows}};
	
	return $self->_run_query( 'batch_mutate', \%mutation_map, $level );
}

=head2 remove_counter

Remove counter C<$column> on C<$key>.

Arguments:

=over 2

column_family, key, column, super_column, consistency_level_write

=back

=cut

sub remove_counter {
	my ($self, @params) = @_;
	my $arguments = {
		@params
	};

	my $level = $self->_consistency_level_write($arguments);

	my $columnPath =
	  Cassandra::ColumnPath->new(
							   {
								 column_family => $arguments->{column_family},
								 super_column  => $arguments->{super_column},
								 column        => $arguments->{column}
							   }
	  );

	return $self->_run_query('remove_counter', $arguments->{key}, $columnPath, $level );
}

=head2 remove

Arguments:

=over 2

column_family, keys, columns, super_column, write_consistency_level

=back

C<$keys> is a key or an I<ARRAY> of keys to be deleted.

A removal whitout keys truncates the whole column_family.

The timestamp used for remove is returned.

=cut

sub remove {
	my ($self, @params) = @_;
	my $arguments = {
		@params
	};
	
	my $timestamp = int (gettimeofday * 1000000);
	my $level     = $self->_consistency_level_write($arguments);

	my $keys = $arguments->{'keys'} ? $arguments->{'keys'} : ( $arguments->{key} ? [$arguments->{key}] : undef );
	
	if ($keys) {

		$keys = [$keys] unless UNIVERSAL::isa( $keys, 'ARRAY' );

		my $deletion =
		  Cassandra::Deletion->new(
							   {
								 timestamp    => $timestamp,
								 super_column => $arguments->{super_column} // undef,
							   }
		  );

		if ( exists $arguments->{columns} ) {
			$deletion->{predicate} = Cassandra::SlicePredicate->new(
										  { column_names => $arguments->{columns} } );
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
			$_ => { $arguments->{column_family} =>
					[ Cassandra::Mutation->new( { deletion => $deletion, } ) ] }
		} @{$keys};
		$self->_run_query('batch_mutate', \%mutation_map, $level );
		return $timestamp;
	} else {
		$self->_run_query('truncate', $arguments->{column_family});
		return 1;
	}
}

=head2 list_keyspace_cfs

Arguments:

=over 2

=back

Returns an HASH of C<< { column_family_name => column_family_type } >> where column family type is either C<Standard> or C<Super>

=cut

sub list_keyspace_cfs {
	my ($self, @params) = @_;
	my $arguments = {
		keyspace => $self->keyspace,
		@params
	};
	my $res = $self->_run_query( 'describe_keyspace', $arguments->{keyspace} )->{cf_defs};	
	return [ map { $_->{name} } @$res ];
}

=head2 create_column_family

Arguments:

=over 2

keyspace, column_family, C<cfdef>

=back

C<cfdef> is any Column Family Definition option (column_type, comparator_type, etc.).

=cut

sub create_column_family {
	my ($self, @params) = @_;
	my $arguments = {
		keyspace => $self->keyspace,
		@params
	};
	$arguments->{name} = $arguments->{column_family};
	my $cfdef = Cassandra::CfDef->new($arguments);

	my $res = $self->_run_query( 'system_add_column_family', $cfdef);
	$self->_wait_for_agreement();
	return $res;
}

=head2 create_keyspace

Arguments:

=over 2

keyspace, strategy, strategy_options

=back

=cut

sub create_keyspace {
	my ($self, @params) = @_;
	my $arguments = {
		keyspace => $self->keyspace,
		@params
	};

	my $params = {};

	$params->{strategy_class} =
	  'org.apache.cassandra.locator.NetworkTopologyStrategy'
	  unless $arguments->{strategy};

	$params->{strategy_options} = { 'datacenter1' => '1' }
	  unless $arguments->{strategy_options};

	$params->{cf_defs} = [];
	$params->{name}    = $arguments->{keyspace};

	my $ksdef = Cassandra::KsDef->new($params);

	my $res = $self->_run_query( 'system_add_keyspace', $ksdef);
	$self->_wait_for_agreement();

	return $res;
}

=head2 list_keyspaces

Arguments:

=over 2

=back

=cut

sub list_keyspaces {
	my ($self) = @_;

	my $cl = $self->pool->get();
	my $res = $self->_run_query( 'describe_keyspaces' );
	return [ map { $_->{name} } @$res ];
}

=head2 drop_keyspace

Arguments:

=over 2

keyspace

=back

=cut

sub drop_keyspace {
	my ($self, @params) = @_;
	my $arguments = {
		keyspace => $self->keyspace,
		@params
	};

	my $res = $self->_run_query('system_drop_keyspace', $arguments->{keyspace});
	$self->_wait_for_agreement();

	return $res;
}

=head2 create_index

Arguments:

=over 2

keyspace, column_family, columns, validation_class

=back

Creates an index on C<$columns> of C<$column_family>.
C<$columns> is an ARRAY of column names to be indexed.
C<$validation_class> only applies when C<$column> doesn't yet exist, and even then it is optional (defaults to I<BytesType>).

=cut

sub create_index {
	my ($self, @params) = @_;
	my $arguments = {
		keyspace => $self->keyspace,
		validation_class => 'org.apache.cassandra.db.marshal.BytesType',
		@params
	};

	my $columns = $arguments->{columns} ? $arguments->{columns}: undef;

	if ( !UNIVERSAL::isa( $columns, 'ARRAY' ) ) {
		$columns = [$columns];
	}

	#get column family definition, substitute the target column with itself but indexed.
	my $cfdef =
		[ grep { $_->{name} eq $arguments->{column_family} }
		   @{ $self->_run_query('describe_keyspace', $arguments->{keyspace})->{cf_defs} } ]->[0];

	my $newmetadata =
	  { map { $_->{name} => $_ } @{ $cfdef->{column_metadata} } };

	foreach my $col ( @{$columns} ) {
		$newmetadata->{$col} =
		  $newmetadata->{$col} // Cassandra::ColumnDef->new(
									   {
										 name             => $col,
										 validation_class => $arguments->{validation_class},
									   }
		  );
		$newmetadata->{$col}->{index_type} = 0;
		$newmetadata->{$col}->{index_name} = join($arguments->{column_family}, $col, "idx");
	}

	$cfdef->{column_metadata} = [ values %$newmetadata ];

	#print Dumper $cfdef;
	my $res = $self->_run_query('system_update_column_family',$cfdef );
	$self->_wait_for_agreement();
	
	return $res;
}

=head2 ring

Arguments:

=over 2

keyspace

=back

Lists the addresses of all nodes on the cluster associated with the keyspace C<<$keyspace>>.

=cut

sub ring {
	my ($self, @params) = @_;
	my $arguments = {
		keyspace => $self->keyspace,
		@params
	};
	
	my @result = 
		map {
			map { $_ } @{ $_->{rpc_endpoints} }
		} @{ $self->_run_query('describe_ring', $arguments->{keyspace}) };

	return \@result;
}

=head1 BUGS

Bugs should be reported on github at L<https://github.com/fmgoncalves/p5-cassandra-simple>.

=cut

=head1 TODO

B<Thrift Type Checking and Packing/Unpacking>

The defined types (or defaults) for each column family are known and should therefore be complied with. 
Introducing Composite Types has forcefully introduced this functionality to an extent, but there should be a refactoring to make this ubiquitous to the client.

B<Error Handling>

Exceptions raised when calling Cassandra code should be reported in error form with appropriate description.

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

describe_partitioner

string describe_partitioner()

describe_snitch

string describe_snitch()

describe_version

string describe_version()

system_drop_column_family

string system_drop_column_family(ColumnFamily column_family)

=back


=cut

=head1 ACKNOWLEDGEMENTS

Implementation loosely based on Cassandra::Lite.

=over 2

L<http://search.cpan.org/~gslin/Cassandra-Lite-0.0.4/lib/Cassandra/Lite.pm>

=back

API based on Pycassa.

=over 2

L<http://pycassa.github.com/pycassa/>

=back

=cut

=head1 AUTHOR

Filipe Gonçalves C<< <the.wa.syndrome@gmail.com> >>

=cut

=head1 COPYRIGHT AND LICENSE

This software is copyright (c) 2012 by Filipe Gonçalves.

This is free software; you can redistribute it and/or modify it under the same terms as the Perl 5 programming language system itself.

=cut

1;
