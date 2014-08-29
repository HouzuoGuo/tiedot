Name:		tiedot
Version:	3.1
Release:	1%{?dist}
License:	BSD-2-Clause
Summary:	A NoSQL document database engine powered by Go
Url:		https://github.com/HouzuoGuo/%{name}
Source:		https://github.com/HouzuoGuo/%{name}/archive/%{version}.tar.gz
Group:		Applications/Databases
BuildRequires:	go git mercurial
Provides:		tiedot
BuildArch:		x86_64
ExcludeArch:	x86

%description
tiedot is a document database engine that uses JSON as document notation; it has a powerful query processor that supports advanced set operations; it can be embedded into your program, or run a stand-alone server using HTTP for an API.

%prep
echo hi

%build

%install

%check

%post

%postun

%files
%defattr(-,root,root)
%doc README.md LICENSE

