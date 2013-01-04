package Cassandra::LazyQuery;

use strict;
use warnings;

use Data::Dumper;

my @row_level_queries = qw/get_range get_indexed_slices/;
my @column_level_queries = qw/get/;

sub new {
	my ($class, $client, $method, @params) = @_;
	my $self = {
		done => 0,
		method => $method,
		client => $client,
		args => {@params}
	};
	if( grep { $_ eq $method } @row_level_queries ){
		$self->{pivot_field} = 'start';
		$self->{limit} = 'row_count';
	} elsif( grep { $_ eq $method } @column_level_queries ){
		$self->{pivot_field} = 'column_start';
		$self->{limit} = 'column_count';
	} else {
		die "Lazy query can only be used with the following methods: ".join(', ',@column_level_queries,@row_level_queries).".";
	}
	$self->{pivot_value} = $self->{args}->{$self->{pivot_field}};
	bless( $self, $class );
	return $self;
}

sub run {
	my ($self, $limit) = @_;
	
	return if $self->{done};
	
	$limit ||= 100;
	$limit += 1 if $self->{pivot_value};
	
	my $method = $self->{method};
	
	my $result = $self->{client}->$method(
		%{$self->{args}},
		$self->{pivot_field} => $self->{pivot_value},
		$self->{limit} => $limit,
	);
	
	if ($self->{pivot_value}) {
		delete $result->{$self->{pivot_value}};
		$limit -= 1;
	}
	
	my @result_keys = keys % $result;

	## Termination conditions (i.e. no more results to be fetched)
	if ( !@result_keys ){
		$self->{done} = 1;
		return;
	} elsif ($limit > scalar @result_keys){
		$self->{done} = 1;
	}
	
	$self->{pivot_value} = pop @result_keys;
	
	return $result;
}

1;