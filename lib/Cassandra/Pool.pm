package Cassandra::Pool;

=pod

=encoding utf8

=head1 NAME

Cassandra::Pool

=head1 DESCRIPTION

Client Pool for Cassandra::Simple

=cut

use strict;
use warnings;

use 5.010;
use Cassandra::Cassandra;
use Cassandra::Types;
use Thrift;
use Thrift::BinaryProtocol;
use Thrift::FramedTransport;
use Thrift::Socket;

use ResourcePool;
use ResourcePool::LoadBalancer;

use Cassandra::Pool::CassandraServer;

use Carp;
use Data::Dumper;

sub new {
	my $class    = shift;
	my $keyspace = shift;
	my $opt      = shift;
	my $self     = {};

	$opt->{keyspace} = $keyspace if $keyspace;

	my $loadbalancer = ResourcePool::LoadBalancer->new(
		"cassandra" . int( rand() * 100000 ),
		MaxTry      => 6,
		SleepOnFail => [ 0, 1 ]
	);

	$loadbalancer->add_pool(
		ResourcePool->new(
			Cassandra::Pool::CassandraServerFactory->new($opt),
			Weight => 100
		)
	);

	$self->{pool}     = $loadbalancer;
	$self->{rcp_opts} = $opt;

	$self = bless( $self, $class );

	return $self;
}

sub add_pool {
	my ( $self, $keyspace, @nodes ) = @_;
	$keyspace = $keyspace || $self->{rcp_opts}->{keyspace};
	croak "No nodes specified" unless scalar @nodes;
	foreach (@nodes) {
		next if $self->{rcp_opts}->{server_name} eq $_;
		my %params = %{ $self->{rcp_opts} };
		$params{server_name} = $_;
		$self->{pool}->add_pool(
			ResourcePool->new(
				Cassandra::Pool::CassandraServerFactory->new( \%params ),
				PreCreate => 2
			),
			,
			Weight => 60
		);
	}
}

sub add_pool_from_ring {
	my $self = shift;
	my $keyspace = shift || $self->{rcp_opts}->{keyspace};
	if ($keyspace) {
		my @nodes     = @{ $self->{pool}->get()->describe_ring($keyspace) };
		my @nodes_ips = map {
			map { split( /\//, $_ ) }
			  @{ $_->{rpc_endpoints} }
		} @nodes;
		$self->add_pool( $keyspace, @nodes_ips );
	}
}

sub get {
	my ( $self, @params ) = @_;
	my $c = $self->{pool}->get(@params);
	return $c;
}

sub put {
	my ( $self, @params ) = @_;
	return $self->{pool}->free(@params);
}

sub fail {
	my ( $self, @params ) = @_;
	return $self->{pool}->fail(@params);
}

#get, put, fail

1;
