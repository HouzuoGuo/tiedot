// +build !amd64

package main

func error() {
	`You should build and run tiedot on x86-64 systems. See buildconstraint.go for more details and workaround.`
}

/*
You should compile/run tiedot on x86-64 systems.

tiedot cannot compile or reliably run on 32-bit systems due to:
- Hash-table key-smear algorithm overflows 32-bit integer and prevents compilation.
- Data files are not split into 2GB chunks.
- Document ID generator involves using a random number source which produces platform integer (32 or 64 bits).

However, you may safely use tiedot on 32-bit systems ONLY IF there is a very small amount of data to be managed - several thousand of documents per collection (at maximum); to do so:
1. Modify hash-table integer-smear algorithm in data/hashtable.go HashKey(int)int function.
2. Run all test cases to make sure that your new algorithm works.
4. Delete buildconstraint.go (this file).
5. Compile and run tiedot.
*/
