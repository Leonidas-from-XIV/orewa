open Core
open Async

let test_get () =
  return ()

let test_set = [
  Alcotest_async.test_case "GET" `Quick test_get;
]

let () =
  Alcotest.run Caml.__MODULE__ [
    ("test_set", test_set);
  ]
