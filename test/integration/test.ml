open Core
open Async

(* This integration test will leak Redis keys left and right *)

let host = "localhost"

module Orewa_error = struct
  type t = [ `Connection_closed | `Eof | `Unexpected] [@@deriving show, eq]
end

let err = Alcotest.testable Orewa_error.pp Orewa_error.equal
let resp = Alcotest.testable Orewa.Resp.pp Orewa.Resp.equal

let re = Alcotest.(result resp err)
let ue = Alcotest.(result unit err)
let ie = Alcotest.(result int err)

let random_state = Random.State.make_self_init ()
let random_key () =
  let random_char _ =
    let max = 126 in
    let min = 33 in
    let random_int = min + Random.State.int random_state (max-min) in
    Char.of_int_exn random_int
  in
  let random_string = String.init 7 ~f:random_char in
  Printf.sprintf "redis-integration-%s" random_string

let test_echo () =
  Orewa.connect ~host @@ fun conn ->
    let message = "Hello" in
    let%bind response = Orewa.echo conn message in
    Alcotest.(check re) "ECHO faulty" (Ok (Orewa.Resp.Bulk message)) response;
    return ()

let test_set () =
  Orewa.connect ~host @@ fun conn ->
    let key = random_key () in
    let%bind res = Orewa.set conn ~key "value" in
    Alcotest.(check ue) "SET failed" (Ok ()) res;
    return ()

let test_set_get () =
  Orewa.connect ~host @@ fun conn ->
    let key = random_key () in
    let value = random_key () in
    let%bind _ = Orewa.set conn ~key value in
    let%bind res = Orewa.get conn key in
    Alcotest.(check re) "Correct response" (Ok (Orewa.Resp.Bulk value)) res;
    return ()

let test_large_set_get () =
  Orewa.connect ~host @@ fun conn ->
    let key = random_key () in
    let ten_mb = 1024 * 1024 * 10 in
    let value = String.init ten_mb ~f:(fun _ -> 'a') in
    let%bind res = Orewa.set conn ~key value in
    Alcotest.(check ue) "Large SET failed" (Ok ()) res;
    let%bind res = Orewa.get conn key in
    Alcotest.(check re) "Large GET retrieves everything" (Ok (Orewa.Resp.Bulk value)) res;
    return ()

let test_lpush () =
  Orewa.connect ~host @@ fun conn ->
    let key = random_key () in
    let value = "value" in
    let%bind res = Orewa.lpush conn ~key value in
    Alcotest.(check ie) "LPUSH did not work" (Ok 1) res;
    return ()

let test_lpush_lrange () =
  Orewa.connect ~host @@ fun conn ->
    let key = random_key () in
    let value = random_key () in
    let value' = random_key () in
    let%bind _ = Orewa.lpush conn ~key value in
    let%bind _ = Orewa.lpush conn ~key value' in
    let%bind res = Orewa.lrange conn ~key ~start:0 ~stop:(-1) in
    Alcotest.(check (result (list string) err)) "LRANGE failed" (Ok [value'; value]) res;
    return ()

let test_set = [
  Alcotest_async.test_case "ECHO" `Slow test_echo;
  Alcotest_async.test_case "SET" `Slow test_set;
  Alcotest_async.test_case "GET" `Slow test_set_get;
  Alcotest_async.test_case "Large SET/GET" `Slow test_large_set_get;
  Alcotest_async.test_case "LPUSH" `Slow test_lpush;
  Alcotest_async.test_case "LRANGE" `Slow test_lpush_lrange;
]

let () =
  Alcotest.run Caml.__MODULE__ [
    ("test_set", test_set);
  ]
