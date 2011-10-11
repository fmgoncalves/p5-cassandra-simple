package Cassandra::Composite;

use strict;
use warnings;

require Exporter;

our @ISA    = qw/ Exporter /;
our @EXPORT = qw/ composite composite_to_array/;

use Data::Dumper;

sub composite {
	my @args = @_;

	#print "Input -> ". Dumper @{$args} ;
	my $res = join "", map { pack( "n", length($_) ) . "$_\x00" } @args;

	#print "Output -> ". $res;
	return $res;
}

sub composite_to_array {
	my $name = shift;

	#print "Input -> ".Dumper $name;
	my $size = length($name);
	my @a;
	my $len = 0;
	do {
		my $off += unpack( 'n', substr $name, $len, $len + 2 );
		push @a, substr $name, $len + 2, $off;
		$len += $off + 3;
	} while ( $len < $size );

	#print "Output ->". Dumper \@a;
	return \@a;
}

1;
