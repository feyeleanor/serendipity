serendipity
===========

A pure Go relational database system ported from and inspired by SQLite 3.

rationale
=========

Starting with a baseline of SQLite 3.7.17 Amalgamation (which introduced memory-mapped I/O) my
aim is to build a relational database engine which can be included in Go projects without
forcing the use of CGO.

By porting the SQLite design to pure Go it should be possible to use native concurrency support
instead of pthreads to better manage multiple database queries, with the hope that this design
will suggest ways in which the engine can be further scaled to support multi-node clustering.

method
======

SQLite is a small but dense codebase and the amalgamation relies on a number of macros to
customise the code at compile time. Go doesn't sit comfortably with macro systems so I'll be
habd cranking these when alternative approaches are unclear. There are also features of SQLite
which can be expressed very differently in Go than in C (such as stripping out pthreads) and
where possible we'll be making these changes atomically.

Another key principle is that for every change to the codebase, there'll be a commit here in
git so that I can explore alternate paths and backtrack if I get in a tangle. I don't usually
code in quite such a disciplined manner so this will be a learning experience...

A key aspect of Go that I want to explore in this project is the use of interfaces so that
where practical other projects can reuse the core serendipity engine whilst providing their
own implementations for particular components.

Underneath SQLite is a very nice Relational Algebra Virtual Machine which I intend to rework
in a similar vein to my GoLightly experiments. This is where the main gains for exploiting
Go's native concurrency and memory management are likely to be found. And yes, that does mean
that interoperating serendipity VMs with those for other systems is part of my road map.

And as a final challenge I'm going to try and play nicely with the standard SQL package so that
serendipity can be a drop-in RDBMS for any project using that interface.

how to help
===========

If you know either Go or the SQLite codebase well and you fancy helping I'd love to hear from
you.

Whilst SQLite is in the public domain its test suite is not, so Serendipity will need a test
suite before it's ready for production use. I am not planning on addressing this aspect until
a substantial portion of the codebase has been converted. I WILL VERY MUCH WELCOME ANY HELP
WITH BUILDING THIS TEST SUIT ASAP.

And if you're somebody who'd like to see this project succeed but who can't help with the coding,
I accept donations via PayPal and will in future be looking for other ways to raise funds so I
can make this a full-time effort.