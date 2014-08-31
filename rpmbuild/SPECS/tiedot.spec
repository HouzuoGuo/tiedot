Name:		tiedot
Version:	3.0
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
go build -o %{name} .

%install
install -d %{buildroot}%{_bindir}
install -p -m 0755 %{name} %{buildroot}%{_bindir}/%{name}

%check
echo this is check

%post

%postun

%files
%defattr(-,root,root)
%doc README.md LICENSE doc
%{_bindir}/tiedot

