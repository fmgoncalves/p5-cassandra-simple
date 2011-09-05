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

use Data::Dumper;


sub new{
	my $class = shift;
	my $keyspace = shift;
	my $opt = shift;
	my $self={};
	
	my $server_name = $opt->{server_name} // '127.0.0.1';
	my $server_port = $opt->{server_port} // 9160;
	
	$self->{username} = $opt->{username} // '';
	$self->{password} = $opt->{password} // '';
	
	$self = bless($self, $class);
	
	my $first = $self->create_client({server_name => $server_name, server_port => $server_port});
	$first->set_keyspace($keyspace);
	
	$self->{pool} = [];
	
	push @{$self->{pool}}, $first;
	
	foreach( map {$_->{endpoints}->[0]} @{$first->describe_ring($keyspace)} ){
		next if $server_name eq $_;
		#print $_," ",$keyspace,"\n";
		my $cl = $self->create_client({server_name =>  $_, server_port => $server_port});
		$cl->set_keyspace($keyspace);
		push @{$self->{pool}}, $cl;
	}
	
	return $self;
}

sub create_client{
	
	my $self = shift;
	my $opt = shift // {};
	my $server_name = $opt->{server_name};
	my $server_port = $opt->{server_port} // 9160;
	
	
	my $sock = Thrift::Socket->new( $server_name, $server_port );
	
	my $transport = Thrift::FramedTransport->new( $sock, 1024, 1024 );
	
	my $protocol = Thrift::BinaryProtocol->new( $transport );
	
	my $client = Cassandra::CassandraClient->new( $protocol );
	
	$transport->open;
	
	my $auth = Cassandra::AuthenticationRequest->new;
	$auth->{credentials} =
	  { username => $self->{username}, password => $self->{password} };

	$client->login($auth);

	return $client;
}

sub get{
	my $self = shift;
	return $self->{pool}->[rand @{$self->{pool}}];
}

1