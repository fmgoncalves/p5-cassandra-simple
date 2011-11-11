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


	$opt->{keyspace}    = $keyspace;

	$self = bless( $self, $class );

	 my $loadbalancer = ResourcePool::LoadBalancer->new("cassandra", MaxTry => 6);#TODO try alternative policy methods
		
	$loadbalancer->add_pool(ResourcePool->new(Cassandra::Pool::CassandraServerFactory->new($opt))); 
 
#	print Dumper map { split( /\//, $_->{endpoints}->[0]) } @{$first->describe_ring($keyspace)};

	foreach ( map { split( /\//, $_->{endpoints}->[0] ) }
			  @{ $loadbalancer->get()->describe_ring($keyspace) } )
	{
		next if $opt->{server_name} eq $_;
		my %params = %$opt;
		$params{server_name} = $_;
		$loadbalancer->add_pool(ResourcePool->new(Cassandra::Pool::CassandraServerFactory->new(\%params), PreCreate => 2));
	}
	$self->{pool} = $loadbalancer;
	
	return $self;
}

sub get{
	my $self = shift;
	return $self->{pool}->get(@_);
}

sub put{
	my $self = shift;
	return $self->{pool}->free(@_);
}

sub fail{
	my $self = shift;
	return $self->{pool}->fail(@_);
}
#get, put, fail

1
