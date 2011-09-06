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

sub new {
	my $class    = shift;
	my $keyspace = shift;
	my $opt      = shift;
	my $self     = {};

	my $server_name = $opt->{server_name} // '127.0.0.1';
	my $server_port = $opt->{server_port} // 9160;

	$self->{username} = $opt->{username} // '';
	$self->{password} = $opt->{password} // '';

	$self = bless( $self, $class );

	$self->{keyspace}    = $keyspace;
	
	my $first = $self->create_client(
				 { server_name => $server_name, server_port => $server_port } );

	$self->{server_port} = $server_port;

	$self->{pool} = {};

	$self->{pool}->{$server_name} = [ $first, 0 ];

#	print Dumper map { split( /\//, $_->{endpoints}->[0]) } @{$first->describe_ring($keyspace)};

	foreach ( map { split( /\//, $_->{endpoints}->[0] ) }
			  @{ $first->describe_ring($keyspace) } )
	{
		next if $server_name eq $_;

		#		print ">> ",$_," ",$keyspace,"\n";
		$self->{pool}->{$_} = [ undef, 0 ];
	}

	return $self;
}

sub create_client {

	my $self        = shift;
	my $opt         = shift // {};
	my $server_name = $opt->{server_name};
	my $server_port = $opt->{server_port} // 9160;

	my $sock = Thrift::Socket->new( $server_name, $server_port );

	my $transport = Thrift::FramedTransport->new( $sock, 1024, 1024 );

	my $protocol = Thrift::BinaryProtocol->new($transport);

	my $client = Cassandra::CassandraClient->new($protocol);

	$transport->open;

	#	print "»opened transport \n";

	my $auth = Cassandra::AuthenticationRequest->new;
	$auth->{credentials} =
	  { username => $self->{username}, password => $self->{password} };

	$client->login($auth);
	$client->set_keyspace( $self->{keyspace} );

	return $client;
}

sub get {
	my $self = shift;

	my @ol =
	  sort { $self->{pool}->{$a}->[1] cmp $self->{pool}->{$b}->[1] } #TODO: obviously not a good solution. should use a priorityqueue or something, not sort the whole pool at every get.
	  keys %{ $self->{pool} };
	my $server_name = shift @ol;
	
	my $cl          = $self->{pool}->{$server_name}->[0];

	while ( (not defined $cl) and scalar @ol ) {
		#print ">> undef $server_name ". defined $cl, "\n";
		eval {
			$cl = $self->create_client(
										{
										  server_name => $server_name,
										  server_port => $self->{server_port}
										}
			);
			$self->{pool}->{$server_name}->[0] = $cl;
		};
		if ($@) {
			$self->{pool}->{$server_name}->[1] += 5;
			$server_name = shift @ol;
			$cl          = $self->{pool}->{$server_name}->[0];
		}
	}
#	print "COUNTER for $server_name -> " . $self->{pool}->{$server_name}->[1], "\n";
	$self->{pool}->{$server_name}->[1] += 1;
	return $cl;
}

sub put {
	my $self = shift;
	my $client = shift;
	
	my $server_name = $client->{input}->{trans}->{transport}->{host};
	
	$self->{pool}->{$server_name}->[1] -= 1;
}

sub fail {
	my $self = shift;
	my $client = shift;
	
	my $server_name = $client->{input}->{trans}->{transport}->{host};
	
	$self->{pool}->{$server_name}->[1] += 5;
	$self->{pool}->{$server_name}->[0] += undef;
}


1
