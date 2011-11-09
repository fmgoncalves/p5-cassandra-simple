package Cassandra::Pool::CassandraServer;

use strict;
use warnings;

use Cassandra::Cassandra;
use Cassandra::Types;
use Thrift;
use Thrift::BinaryProtocol;
use Thrift::FramedTransport;
use Thrift::Socket;

use Data::Dumper;

use base qw/ResourcePool::Resource/;

sub new {
	my $class = shift;
	my $opt   = shift;

	my $self = {};

	my $server_name = $opt->{server_name} || '127.0.0.1';
	my $server_port = $opt->{server_port} || 9160;

	my $sock = Thrift::Socket->new( $server_name, $server_port );

	my $transport = Thrift::FramedTransport->new( $sock, 1024, 1024 );

	my $protocol = Thrift::BinaryProtocol->new($transport);

	$self->{client} = Cassandra::CassandraClient->new($protocol);

	$transport->open;

	my $auth = Cassandra::AuthenticationRequest->new;
	$auth->{credentials} = {
							 username => $opt->{username} || '',
							 password => $opt->{password} || ''
	};

	$self->{client}->login($auth);
	$self->{client}->set_keyspace( $opt->{keyspace} );

	bless( $self, $class );

	return $self;
}

=head
Closes a connection gracefully.
=cut

sub close {
	my $self = shift;
	$self->{client}->{input}->getTransport()->close();
	eval { $self->{client}->{output}->getTransport()->close(); };
}

=head
Closes a failed connection and ignores error (since this connection is known to be broken)
=cut

sub fail_close {
	my $self = shift;
	eval {
		$self->{client}->{input}->getTransport()->close();
		$self->{client}->{output}->getTransport()->close();
	};
}

=head
Returns the naked resource which can be used by the client. This is the DBI or Net::LDAP handle for example.

Returns: a reference to a object
=cut

sub get_plain_resource {
	my $self = shift;
	return $self->{client};
}

=head
Checks a connection. 
=cut

sub check {
	my $self = shift;
	return $self->{client}->{input}->getTransport()->isOpen()
	  && $self->{client}->{output}->getTransport()->isOpen();
}

=head
Checks a connection. This method is called by the get() method of the ResourcePool before it returns a connection. The default implementation always returns true.

Returns: true if the connection is valid
=cut

sub precheck {
	return check(@_);
}

=head
Checks a connection. This method is called by the free() method of the ResourcePool to check if a connection is still valid. The default implementation always returns true.

Returns: true if the connection is valid
=cut

sub postcheck {
	return check(@_);
}

##Factory class
package Cassandra::Pool::CassandraServerFactory;

use Data::Dumper;

use base qw/ResourcePool::Factory/;

=head
The new method is called to create a new factory.

Usually this method just stores the parameters somewhere and will use it later create_resource is called.
=cut

sub new {
	my ( $class, $params ) = @_;

	die 'A keyspace must be provided' unless $params->{keyspace};

	my $self = {};
	$self->{params} = $params;
	bless( $self, $class );
	return $self;
}

=head
This method is used to actually create a resource according to the parameters given to the new method.

You must override this method in order to do something useful.

Returns: a reference to a ResourcePool::Resource object
=cut

sub create_resource {
	my $self = shift;

	return new Cassandra::Pool::CassandraServer( $self->{params} );
}

#TODO implement remaining methods if needed only
#$factory->info
#$factory->mk_singleton_key
#$factory->singleton

1;
