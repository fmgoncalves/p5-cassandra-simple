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

use Data::Dumper;

sub new {
	my $class    = shift;
	my $keyspace = shift;
	my $opt      = shift;
	my $self     = {};

	$opt->{keyspace} = $keyspace if $keyspace;

	my $loadbalancer =
	  ResourcePool::LoadBalancer->new( "cassandra" . int( rand() * 100000 ),
							 MaxTry => 6 ); #TODO try alternative policy methods

	$loadbalancer->add_pool(
		 ResourcePool->new( Cassandra::Pool::CassandraServerFactory->new($opt) )
	);
	$self->{pool} = $loadbalancer;
	$self->{rcp_opts} = $opt;

	$self = bless( $self, $class );

	return $self;
}

sub add_pool_from_ring {
	my $self = shift;
	my $keyspace = $self->{rcp_opts}->{keyspace};

	if ($keyspace) {
		my @nodes = @{ $self->{pool}->get()->describe_ring($keyspace) };
		foreach (
			map {
				map { split( /\//, $_ ) } @{ $_->{rpc_endpoints} }
			} @nodes
		  )
		{
			next if $self->{rcp_opts}->{server_name} eq $_;
			my %params = %{$self->{rcp_opts}};

			$params{server_name} = $_;
			$self->{pool}->add_pool(
				   ResourcePool->new(
					   Cassandra::Pool::CassandraServerFactory->new( \%params ),
					   PreCreate => 2
				   )
			);
		}
	}
	
	$self->{pool} = $loadbalancer;

	$self = bless( $self, $class );

	return $self;
}

sub get {
	my $self = shift;
	my $c    = $self->{pool}->get(@_);
	return $c;
}

sub put {
	my $self = shift;
	return $self->{pool}->free(@_);
}

sub fail {
	my $self = shift;
	return $self->{pool}->fail(@_);
}

#get, put, fail

1
