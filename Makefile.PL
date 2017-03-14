use ExtUtils::MakeMaker;

# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.
WriteMakefile(
    NAME           => 'Gearman::Server',
    VERSION_FROM   => 'lib/Gearman/Server.pm',
    ABSTRACT_FROM  => 'lib/Gearman/Server.pm',
    EXE_FILES      => ['bin/gearmand'],
    BUILD_REQUIRES => {
        "File::Spec"       => 0,
        "FindBin"          => 0,
        "IO::Socket::INET" => 0,
        "Net::EmptyPort"   => 0,
        "Proc::Guard"      => 0,
        "Socket"           => 0,
        "Sys::Hostname"    => 0,
        "Test::Exception"  => 0,
        "Test::More"       => 0,
        "Test::Script"     => 1.12,
        "Test::TCP"        => 0,
        "version"          => 0,
    },
    PREREQ_PM => {
        "Danga::Socket"    => 1.52,
        "Gearman::Util"    => 0,
        "IO::Socket::INET" => 0,
        "Sys::Hostname"    => 0,
        "version"          => 0,
    },
    AUTHOR =>
        'Brad Fitzpatrick (brad@danga.com), Brad Whitaker (whitaker@danga.com)',
    META_MERGE => {
        'meta-spec' => { version => 2 },
        resources   => {
            repository => {
                type => 'git',
                url  => 'https://github.com/p-alik/Gearman-Server.git',
                web  => 'https://github.com/p-alik/Gearman-Server',
            },
        },
    },

);
