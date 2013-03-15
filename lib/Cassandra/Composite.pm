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
	
	@args = grep { defined($_) } @args;
	my $res = join "", map {
		my $component = $_ || '';
		my $eoc       = "\x00";
		if (UNIVERSAL::isa( $_, 'ARRAY' ) ) {
			( $component, my $last, my $inclusive ) = @{$component};
			$eoc = !$inclusive ^ !$last ? "\xff" : "\x01";
		}
		use bytes;
		pack( "n", length($component) ) . $component . $eoc;

	} @args;

	return $res;
}

sub composite_to_array {
	use bytes;
	my $name = shift;
	my @ret = ();

	my $size = length($name);

	if (! $size) {
		return \@ret;
	}

	my @bytes = split //, $name;
	my $len = 0;
	while ( $len < $size ) {
		my $off = unpack( 'n', $bytes[$len].$bytes[$len+1] );

		my $comp = '';
		for(1..$off) {
			my $i = $_;
			$comp .= $bytes[$len+1+$i];
		}
		
		push @ret, $comp;
		$len += $off + 3;
	}

	
	return \@ret;
}

1;
