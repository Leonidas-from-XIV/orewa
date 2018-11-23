open Core
open Async

(* This integration test will leak Redis keys left and right *)

let host = "localhost"
let exceeding_read_buffer = 128 * 1024

module Orewa_error = struct
  type t = [ `Connection_closed | `Eof | `Unexpected | `Redis_error of string] [@@deriving show, eq]
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
    let%bind () = after Time.Span.(expire / 0.75) in
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

let test_append () =
  Orewa.connect ~host @@ fun conn ->
    let key = random_key () in
    let value = random_key () in
    let%bind res = Orewa.append conn ~key value in
    Alcotest.(check (result int err)) "APPEND unexpected" (Ok (String.length value)) res;
    return ()

let test_auth () =
  Orewa.connect ~host @@ fun conn ->
    let password = random_key () in
    let%bind res = Orewa.auth conn password in
    let expected = Error (`Redis_error "ERR Client sent AUTH, but no password is set") in
    Alcotest.(check (result unit err)) "AUTH failed" expected res;
    return ()

let test_bgrewriteaof () =
  Orewa.connect ~host @@ fun conn ->
    let%bind res = Orewa.bgrewriteaof conn in
    let expected = Ok "Background append only file rewriting started" in
    Alcotest.(check (result string err)) "BGREWRITEAOF failed" expected res;
    let%bind _ = Deferred.ok @@ after (Time.Span.of_sec 1.) in
    return ()

let test_bgsave () =
  Orewa.connect ~host @@ fun conn ->
    let%bind res = Orewa.bgsave conn in
    let expected = Ok "Background saving started" in
    Alcotest.(check (result string err)) "BGSAVE failed" expected res;
    return ()

let test_bitcount () =
  Orewa.connect ~host @@ fun conn ->
    let key = random_key () in
    let%bind _ = Orewa.set conn ~key "aaaa" in
    let%bind res = Orewa.bitcount conn key in
    Alcotest.(check (result int err)) "BITCOUNT failed" (Ok 12) res;
    let%bind res = Orewa.bitcount conn ~range:(1, 2) key in
    Alcotest.(check (result int err)) "BITCOUNT failed" (Ok 6) res;
    return ()

let tests = Alcotest_async.[
  test_case "ECHO" `Slow test_echo;
  test_case "SET" `Slow test_set;
  test_case "GET" `Slow test_set_get;
  test_case "Large SET/GET" `Slow test_large_set_get;
  test_case "SET with expiry" `Slow test_set_expiry;
  test_case "LPUSH" `Slow test_lpush;
  test_case "LRANGE" `Slow test_lpush_lrange;
  test_case "Large LRANGE" `Slow test_large_lrange;
  test_case "APPEND" `Slow test_append;
  test_case "AUTH" `Slow test_auth;
  test_case "BGREWRITEAOF" `Slow test_bgrewriteaof;
  test_case "BGSAVE" `Slow test_bgsave;
  test_case "BITCOUNT" `Slow test_bitcount;
]

let () =
  Alcotest.run Caml.__MODULE__ [
    ("integration", tests);
  ]
