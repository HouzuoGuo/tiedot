Name:           tiedot
Version:        3.2
Release:        1%{?dist}
License:        BSD-2-Clause
Summary:        A NoSQL document database engine powered by Go
# tiedot is developed at https://github.com/HouzuoGuo/tiedot
# however the distributable source archive is located at https://github.com/tiedot/tiedot
Url:            https://github.com/HouzuoGuo/%{name}
Source:         %{name}-%{version}.tar.gz
Group:          Applications/Databases
BuildRequires:  go,systemd
Requires:       curl,%{?systemd_requires}
ExcludeArch:    x86

%description
A document database engine that uses JSON as document notation; it has a powerful query processor that supports advanced set operations; it can be embedded into your program, or run a stand-alone server using HTTP for an API.

%prep
%setup -q
ln -s src/github.com/HouzuoGuo/%{name} prjsrc

%build
export GOPATH=`pwd`
cd prjsrc
go build -o %{name} .

%install
install -d %{buildroot}%{_bindir}
install -p -m 0755 prjsrc/%{name} %{buildroot}%{_bindir}/%{name}
install -d %{buildroot}%{_sysconfdir}
install -p -m 0644 prjsrc/distributable/etc/%{name} %{buildroot}%{_sysconfdir}
install -d %{buildroot}%_unitdir
install -p -m 0644 prjsrc/distributable/%{name}.service %{buildroot}%_unitdir/%{name}.service
install -d %{buildroot}%{_sbindir}
ln -s /usr/sbin/service %{buildroot}%{_sbindir}/rc%{name}

%check
export GOPATH=`pwd`
cd prjsrc
go test -v example_test.go

%pre
%service_add_pre %{name}.service

%post
%service_add_post %{name}.service

%preun
%service_del_preun %{name}.service

%postun
%service_del_postun %{name}.service

%files
%defattr(-,root,root)
%doc prjsrc/LICENSE prjsrc/doc/*
%config /etc/%{name}
%{_bindir}/%{name}
%_unitdir/%{name}.service
/usr/sbin/rc%{name}

%changelog
