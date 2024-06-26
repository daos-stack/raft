# doesn't seem to work on sles 12.3: %%{!?make_build:%%define make_build %%{__make} %%{?_smp_mflags}}
# so...
%if 0%{?suse_version} <= 1320
%define make_build  %{__make} %{?_smp_mflags}
%endif

%bcond_with use_release

%global debug_package %{nil}

Name:		raft
Version:	0.11.0
Release:	1%{?relval}%{?dist}

Summary:	C implementation of the Raft Consensus protocol, BSD licensed
Provides:	daos-raft = %version-%release%{?dist}

License:	BSD
URL:		https://github.com/daos-stack/%{name}
Source0:	https://github.com/daos-stack/%{name}/releases/download/%{shortcommit0}/%{name}-%{version}.tar.gz

%if 0%{?suse_version} >= 1315
Group:		Development/Libraries/C and C++
%endif

BuildRequires:	make, gcc

%description
Raft is a consensus algorithm that is designed to be easy to understand.
It's equivalent to Paxos in fault-tolerance and performance. The difference
is that it's decomposed into relatively independent subproblems, and it
cleanly addresses all major pieces needed for practical systems.

%package devel
Summary:	Development libs
Provides:   daos-raft-devel = %version-%release

%description devel
Development libs for Raft consensus protocol

%prep
%setup -q

%build
# only build the static lib
%make_build GCOV_CCFLAGS= static

%install
mkdir -p %{buildroot}/%{_libdir}
cp -a libraft.a %{buildroot}/%{_libdir}
mkdir -p %{buildroot}/%{_includedir}
cp -a include/* %{buildroot}/%{_includedir}

%files
%defattr(-,root,root,-)
%doc README.rst
%license LICENSE

%files devel
%defattr(-,root,root,-)
%doc README.rst
%{_libdir}/*
%{_includedir}/*


%changelog
* Wed Feb 14 2024 Li Wei <wei.g.li@intel.com> -0.11.0-1
- Add raft_cbs_t.get_rand
- Introduce logging levels

* Thu Jun 22 2023 Brian J. Murrell <brian.murrell@intel> -0.10.1-2
- Add BR: make, gcc

* Wed Jun 07 2023 Li Wei <wei.g.li@intel.com> -0.10.1-1
- Fix conflicts in a unit test

* Mon Jun 05 2023 Li Wei <wei.g.li@intel.com> -0.10.0-1
- Add leadership lease
- Let leaders step down voluntarily when they can't maintain leases from majority

* Mon Feb 13 2023 Li Wei <wei.g.li@intel.com> -0.9.2-1
- Fix assertion failures in raft_recv_requestvote

* Fri Nov 25 2022 Brian J. Murrell <brian.murrell@intel> -0.9.1-2
- Change BuildArchitectures: noarch

* Wed Apr 06 2022 Li Wei <wei.g.li@intel.com> -0.9.1-1
- Fix membership changes
- Fix node ID initialization

* Wed Jan 05 2022 Li Wei <wei.g.li@intel.com> -0.9.0-1
- Remove the upstream optimization that allows election-less leaders
- Update packaging

* Mon Aug 30 2021 Li Wei <wei.g.li@intel.com> -0.8.1-1
- Optimize InstallSnapshot performance
- Update packaging

* Mon May 31 2021 Li Wei <wei.g.li@intel.com> -0.8.0-1
- Add Pre-Vote
- Set version-release for daos-raft* packages

* Mon Apr 26 2021 Brian J. Murrell <brian.murrell@intel> -0.7.3-2
- Provides daos-raft to avoid getting other raft packages
- disable debug{info,source} package builds

* Wed Feb 24 2021 Li Wei <wei.g.li@intel.com> -0.7.3-1
- Fix disruptions from removed replicas

* Tue Feb 09 2021 Kenneth Cain <kenneth.c.cain@intel> - 0.7.2-1
- Fix more Coverity issues in test_server

* Wed Dec 02 2020 Li Wei <wei.g.li@intel.com> -0.7.1-1
- Fix Coverity issues

* Tue Nov 10 2020 Li Wei <wei.g.li@intel.com> -0.7.0-1
- Use 63-bit log indices and terms

* Fri May 15 2020 Kenneth Cain <kenneth.c.cain@intel> -0.6.0-1
- Expose raft_election_start function

* Fri Apr 10 2020 Brian J. Murrell <brian.murrell@intel> -0.5.0-4
- Add GCOV_CCFLAGS= to static build

* Thu Apr 09 2020 Brian J. Murrell <brian.murrell@intel> -0.5.0-3
- Only build the static library

* Fri Oct 04 2019 John E. Malmberg <john.e.malmberg@intel> -0.5.0-2
- SUSE rpmlint fixups

* Mon Apr 08 2019 Brian J. Murrell <brian.murrell@intel> -0.5.0-1
- initial package
