Dumbo
=======

Introduction
------------

Dumbo is an experimental MPP database engine as-a-service node build using Seastar, Memoria and LLVM JIT.

* [Seastar](http://www.seastar-project.org/) is an event-driven framework providing fast lock-free 
	inter-thread communication, Asynchrous IO for SSDs and network, and fibers.
* [Memoria](https://bitbucket.org/vsmirnov/memoria/wiki/Home) is a C++14 framework providing 
	general purpose dynamic composable data structures on top of key-value, object, file, block or in-memory storage. 
	Out of the box the following data structures are provided: map, vector, multimap, 
	table, wide table and others. 
* [LLVM JIT](http://llvm.org/) to compile queries, filters, transformers and other logic over Memoria's 
	data structures dynamically, on the fly.

In the wild Dumbo is an adorable [deep sea octopus](http://oceana.org/marine-life/cephalopods-crustaceans-other-shellfish/dumbo-octopuses).

Building Dumbo
--------------------

1. Check and follow Seastar's [build instructions](https://github.com/victor-smirnov/seastar#building-seastar) to build it.

2. Copy mkbuild.sh-template to mkbuild.sh and build.sh-template to build.sh. Add the files executable permissions.

3. Edit mkbuild.sh to specify configurable parameters.

4. Run mkbuild.sh

5. Run build.sh