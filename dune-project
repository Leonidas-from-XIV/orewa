(lang dune 1.10)
(using fmt 1.1)
(name orewa)
(generate_opam_files true)
(maintainers "Marek Kubica <marek@xivilization.net>")
(authors "Marek Kubica <marek@xivilization.net>")
(source (github Leonidas-from-XIV/orewa))
(bug_reports "https://github.com/Leonidas-from-XIV/orewa/issues")
(documentation "https://leonidas-from-xiv.github.io/orewa/")
(license LGPL-3.0-or-later)

(package
  (name orewa)
  (depends
    (async (>= v0.11))
    (core (>= v0.11))
    (dune (>= 1.10))
    (ppx_let (>= v0.11))
    (alcotest (and :with-test (>= 1.0.0)))
    (alcotest-async (and :with-test (>= 1.0.0)))
    (fmt (and :with-test (>= 0.8.6)))
    (ppx_deriving (>= 4.2)))
  (synopsis "Async-friendly Redis client")
  (description "Async-friendly Redis client

Orewa is a Redis client designed with cooperative multithreading in mind, thus
operations are non-blocking by default.

It also features an OCaml-friendly Redis API wrapping the stringly constructs
of the Redis commands into a more typed and less fragile interface that does
its best to provide a pleasant interface."))
