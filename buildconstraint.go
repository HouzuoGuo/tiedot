// +build !amd64

package main

func error() {
	`You should build and run tiedot on x86-64 systems. See buildconstraint.go for more details and workaround.`
}

/*
You should compile/run this version of tiedot on x86-64 systems.

Please check out branch "32-bit" for an older version compatible with 32-bit systems.

Please note that "32-bit" branch uses a unique data structure incompatible with any other tiedot releases.
*/
