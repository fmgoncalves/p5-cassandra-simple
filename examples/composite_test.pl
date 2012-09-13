#! /usr/bin/perl -l

#use common::sense;
use strict;
use warnings;
use Carp::Always;

use Data::Dumper;

use Cassandra::Composite qw/composite composite_to_array/;

$|++;

my @test = (
	 [ 'a', 'undef', 44, 'b' ] ,
	 [ 'a', 'undef', 44, 'b', undef ] ,
	 [ undef, 'undef', 44, 'b' ] ,
	 [] ,
	 [ undef ],
	 [ '00000-00000-00000-00000', undef, 44 ],
	 [ 0 ],
);

for my $elem (@test){
	my $composite = composite(@$elem);
	print Dumper composite_to_array($composite);
	print "--------";
}