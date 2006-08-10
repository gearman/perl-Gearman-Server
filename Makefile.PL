use ExtUtils::MakeMaker;
# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.
WriteMakefile(
              NAME         => 'gearmand',
              VERSION_FROM => 'gearmand', # finds $VERSION
              EXE_FILES    => ['gearmand'],
              PREREQ_PM    => {
                               'Gearman::Util' => 0,
                               'Danga::Socket' => 1.52,
                              },

              ( $] >= 5.005 ?
                (
                 AUTHOR       => 'Brad Fitzpatrick (brad@danga.com), Brad Whitaker (whitaker@danga.com)',
                 ABSTRACT => 'gearmand is the multiplexer between gearman workers and clients',
                ) : ()),
              dist => {
                       COMPRESS => 'gzip -9',
                       SUFFIX   => 'gz',
                      },
             );
