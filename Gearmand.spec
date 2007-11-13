name:      Gearmand
summary:   Gearmand - Gearman job distribution server
version:   1.09
release:   2
vendor:    Brad Fitzpatrick <brad@danga.com>
packager:  Jonathan Steinert <hachi@cpan.org>
license:   Artistic
group:     Applications/CPAN
buildroot: %{_tmppath}/%{name}-%{version}-%(id -u -n)
buildarch: noarch
source:    Gearman-Server-%{version}.tar.gz
buildrequires: perl-Danga-Socket >= 1.52, perl-Gearman-Client
requires:  perl-Gearman-Server = %{version}-%{release}
autoreq: no

%description
Gearman job distribution system

%prep
rm -rf "%{buildroot}"
%setup -n Gearman-Server-%{version}

%build
%{__perl} Makefile.PL PREFIX=%{buildroot}%{_prefix}
make all
make test

%install
make pure_install

[ -x /usr/lib/rpm/brp-compress ] && /usr/lib/rpm/brp-compress


# remove special files
find %{buildroot} \(                    \
       -name "perllocal.pod"            \
    -o -name ".packlist"                \
    -o -name "*.bs"                     \
    \) -exec rm -f {} \;

# no empty directories
find %{buildroot}%{_prefix}             \
    -type d -depth -empty               \
    -exec rmdir {} \;

%clean
[ "%{buildroot}" != "/" ] && rm -rf %{buildroot}

%files
%defattr(-,root,root)
%{_prefix}/bin/*
%{_prefix}/share/man/man1

%package -n perl-Gearman-Server
summary:   perl-Gearman-Server - Gearman server libraries.
group:     Applications/CPAN
requires:  perl-Danga-Socket >= 1.52, perl-Gearman-Client
autoreq: no
%description -n perl-Gearman-Server
Gearman server libraries.

%files -n perl-Gearman-Server
%{_prefix}/lib/*
%{_prefix}/share/man/man3
