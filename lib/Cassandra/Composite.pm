package Cassandra::Composite;

use strict;
use warnings;

require Exporter;

our @ISA    = qw/ Exporter /;
our @EXPORT = qw/ composite composite_to_array/;

use Data::Dumper;

=head2 composite

Usage: C<< composite($component [, $component [, ...]]) >>

C<$component> can be one of two things: a basic data type (string, int, float, etc.) or an ARRAY.
The ARRAY format is used when composite is either a column_start or column_finish parameter.
In case it is an ARRAY it must be composed of its value, whether it pertains to a column_finish and whether it is inclusive or not.
	
Returns a string with the encoded composite type from the given components.

=cut

sub composite {
	my @args = @_;

	#print "Input -> ". Dumper @{$args} ;
	my $res = join "", map {
		my $component = $_;
		my $eoc       = "\x00";
		if (UNIVERSAL::isa( $_, 'ARRAY' ) ) {
			( $component, my $last, my $inclusive ) = @{$component};
			$eoc = !$inclusive ^ !$last ? "\xff" : "\x01";
		}
		pack( "n", length($component) ) . $component . $eoc;

	} @args;

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
