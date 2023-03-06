use 5.008006;
use ExtUtils::MakeMaker;
# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.
WriteMakefile(
    NAME              => 'Cassandra::Simple',
    VERSION_FROM      => 'lib/Cassandra/Simple.pm', # finds $VERSION
    LICENSE           => 'perl',
    PREREQ_PM         => {
        'Moose' => '0',
        'Data::Dumper' => '0',
		'Tie::IxHash' => '0',
		'Try::Tiny' => '0',
        'Class::Accessor' => '0',
        'Thrift' => '0',
        'Thrift::XS' => '0',
        'ResourcePool' => '0',
        'constant' => '0'
    }, # e.g., Module::Name => 1.1
    ($] >= 5.005 ?     ## Add these new keywords supported since 5.005
      (
	ABSTRACT => '',
       AUTHOR         => 'Filipe Gonçalves <the.wa.syndrome@gmail.com>') : ()),
);
