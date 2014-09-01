Name:		tiedot
Version:	3.1
Release:	1%{?dist}
License:	BSD-2-Clause
Summary:	A NoSQL document database engine powered by Go
Url:		https://github.com/HouzuoGuo/%{name}
# The source URL points to a tiedot release hosted on Github.
# Before placing the downloaded source into ~/rpmbuild/SOURCES,
# please remember to rename the archive file into %{version}.tar.gz
Source:		https://github.com/HouzuoGuo/%{name}/archive/%{version}.tar.gz
Group:		Applications/Databases
BuildRequires:	go git mercurial
Requires:		/usr/bin/curl
Provides:		tiedot
BuildArch:		x86_64
ExcludeArch:	x86

%description
tiedot is a document database engine that uses JSON as document notation; it has a powerful query processor that supports advanced set operations; it can be embedded into your program, or run a stand-alone server using HTTP for an API.

%prep
%setup -q

%build
export GOPATH=`pwd`/gopath
mkdir -p $GOPATH/src/github.com/HouzuoGuo/
ln -s `pwd` $GOPATH/src/github.com/HouzuoGuo/%{name}
go get -d
go build -o %{name} .

%install
install -d %{buildroot}%{_bindir}
install -d %{buildroot}/usr/lib/systemd/user
install -p -m 0755 %{name} %{buildroot}%{_bindir}/%{name}
install -p -m 0644 systemd-init/%{name}.service %{buildroot}/usr/lib/systemd/user/%{name}.service

%check

%post

%postun

%files
%defattr(-,root,root)
%doc README.md LICENSE doc
%{_bindir}/%{name}
/usr/lib/systemd/user/%{name}.service

