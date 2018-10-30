open Core
open Async

let host = "localhost"

let resp = Alcotest.testable Orewa.Resp.pp Orewa.Resp.equal

let test_echo () =
  Orewa.connect ~host @@ fun conn ->
    let message = "Hello" in
    match%bind Orewa.echo conn message with
    | Ok response ->
      Alcotest.(check resp) "Correct response" (Orewa.Resp.Bulk message) response;
      return ()
    | Error _ ->
      Alcotest.(check bool) "Did not get response" true false;
      return ()

let test_set = [
  Alcotest_async.test_case "ECHO" `Quick test_echo;
]

let () =
  Alcotest.run Caml.__MODULE__ [
    ("test_set", test_set);
  ]
