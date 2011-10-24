use 5.008006;
use ExtUtils::MakeMaker;
# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.
WriteMakefile(
    NAME              => 'Cassandra::Simple',
    VERSION_FROM      => 'lib/Cassandra/Simple.pm', # finds $VERSION
    PREREQ_PM         => {
        'Any::Moose' => '0',
        'Data::Dumper' => 0,
        'Class::Accessor' => '0',
        'Thrift' => '0',
        'Thrift::BinaryProtocol' => '0',
        'Thrift::FramedTransport' => '0',
        'Thrift::Socket' => '0',
        'constant' => '0'
    }, # e.g., Module::Name => 1.1
    ($] >= 5.005 ?     ## Add these new keywords supported since 5.005
      (ABSTRACT_FROM  => 'lib/Cassandra/Simple.pm',
       AUTHOR         => 'Filipe Gonçalves <the.wa.syndrome@gmail.com>') : ()),
);