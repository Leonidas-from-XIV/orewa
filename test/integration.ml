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
      Alcotest.(check bool) "Did not get desired response" true false;
      return ()

let test_set () =
  Orewa.connect ~host @@ fun conn ->
    match%bind Orewa.set conn ~key:"key" "value" with
    | Ok () ->
      Alcotest.(check unit) "Correct response" () ();
      return ()
    | Error _ ->
      Alcotest.(check bool) "Did not get desired response" true false;
      return ()

let test_set_get () =
  Orewa.connect ~host @@ fun conn ->
    let key = "key" in
    let value = "value" in
    match%bind Orewa.set conn ~key value with
    | Error _ ->
      Alcotest.(check bool) "Did not get desired response" true false;
      return ()
    | Ok () ->
      let%bind res = Orewa.get conn key in
      match res with
      | Error _ ->
        Alcotest.(check bool) "Did not get desired response" true false;
        return ()
      | Ok res ->
        Alcotest.(check resp) "Correct response" (Orewa.Resp.Bulk value) res;
        return ()

let test_lpush () =
  Orewa.connect ~host @@ fun conn ->
    let key = "list" in
    let value = "value" in
    match%bind Orewa.lpush conn ~key value with
    | Error _ ->
      Alcotest.(check bool) "Did not get desired response" true false;
      return ()
    | Ok length ->
      Alcotest.(check int) "Did not get desired length" 1 length;
      return ()

let test_lpush_lrange () =
  Orewa.connect ~host @@ fun conn ->
    let key = "list" in
    let value = "value" in
    match%bind Orewa.lpush conn ~key value with
    | Error _ ->
      Alcotest.(check bool) "Did not get desired response" true false;
      return ()
    | Ok _ ->
      match%bind Orewa.lrange conn ~key ~start:0 ~stop:(-1) with
      | Error _ ->
        Alcotest.(check bool) "Did not get desired response" true false;
        return ()
      | Ok res ->
        Alcotest.(check resp) "Correct response" (Orewa.Resp.Array [Orewa.Resp.Bulk value]) res;
        return ()

let test_set = [
  Alcotest_async.test_case "ECHO" `Slow test_echo;
  Alcotest_async.test_case "SET" `Slow test_set;
  Alcotest_async.test_case "GET" `Slow test_set_get;
  (* Alcotest_async.test_case "LPUSH" `Slow test_lpush; *)
  Alcotest_async.test_case "LRANGE" `Slow test_lpush_lrange;
]

let () =
  Alcotest.run Caml.__MODULE__ [
    ("test_set", test_set);
  ]
