open Core
open Async

(* This integration test will leak Redis keys left and right *)

let host = "localhost"
let exceeding_read_buffer = 128 * 1024

module Orewa_error = struct
  type t = [ `Connection_closed | `Eof | `Unexpected] [@@deriving show, eq]
end

let err = Alcotest.testable Orewa_error.pp Orewa_error.equal

let ue = Alcotest.(result unit err)
let ie = Alcotest.(result int err)
let se = Alcotest.(result string err)
let soe = Alcotest.(result (option string) err)

let truncated_string_pp formatter str =
  let str = Printf.sprintf "%s(...)" (String.prefix str 10) in
  Format.pp_print_text formatter str

let truncated_string = Alcotest.testable truncated_string_pp String.equal

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
    Alcotest.(check se) "ECHO faulty" (Ok message) response;
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
    Alcotest.(check soe) "Correct response" (Ok (Some value)) res;
    return ()

let test_set_expiry () =
  Orewa.connect ~host @@ fun conn ->
    let key = random_key () in
    let value = random_key () in
    let expire = Time.Span.of_ms 200. in
    let%bind res = Orewa.set conn ~key ~expire value in
    Alcotest.(check ue) "Correctly SET expiry" (Ok ()) res;
    let%bind res = Orewa.get conn key in
    Alcotest.(check soe) "Key still exists" (Ok (Some value)) res;
    let%bind () = after expire in
    let%bind res = Orewa.get conn key in
    Alcotest.(check soe) "Key has expired" (Ok None) res;
    return ()


let test_large_set_get () =
  Orewa.connect ~host @@ fun conn ->
    let key = random_key () in
    let value = String.init exceeding_read_buffer ~f:(fun _ -> 'a') in
    let%bind res = Orewa.set conn ~key value in
    Alcotest.(check ue) "Large SET failed" (Ok ()) res;
    let%bind res = Orewa.get conn key in
    Alcotest.(check soe) "Large GET retrieves everything" (Ok (Some value)) res;
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
    Alcotest.(check (result (list truncated_string) err)) "LRANGE failed" (Ok [value'; value]) res;
    return ()

let test_large_lrange () =
  Orewa.connect ~host @@ fun conn ->
    let key = random_key () in
    let value = String.init exceeding_read_buffer ~f:(fun _ -> 'a') in
    let values = 5 in
    let%bind expected = Deferred.List.init values ~f:(fun _ ->
      let%bind _ = Orewa.lpush conn ~key value in
      return value)
    in
    let%bind res = Orewa.lrange conn ~key ~start:0 ~stop:(-1) in
    Alcotest.(check (result (list truncated_string) err)) "LRANGE failed" (Ok expected) res;
    return ()

let tests = [
  Alcotest_async.test_case "ECHO" `Slow test_echo;
  Alcotest_async.test_case "SET" `Slow test_set;
  Alcotest_async.test_case "GET" `Slow test_set_get;
  Alcotest_async.test_case "Large SET/GET" `Slow test_large_set_get;
  Alcotest_async.test_case "SET with expiry" `Slow test_set_expiry;
  Alcotest_async.test_case "LPUSH" `Slow test_lpush;
  Alcotest_async.test_case "LRANGE" `Slow test_lpush_lrange;
  Alcotest_async.test_case "Large LRANGE" `Slow test_large_lrange;
]

let () =
  Alcotest.run Caml.__MODULE__ [
    ("integration", tests);
  ]
