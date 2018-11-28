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
let some_string = Alcotest.testable String.pp (const (const true))

let unordered_string_list = Alcotest.(testable (pp (list string)) (fun a b ->
  let equal = equal (list string) in
  let compare = String.compare in
  equal  (List.sort ~compare a) (List.sort ~compare b)))

let truncated_string_pp formatter str =
  let str = Printf.sprintf "%s(...)" (String.prefix str 10) in
  Format.pp_print_text formatter str

let truncated_string = Alcotest.testable truncated_string_pp String.equal

let random_state = Random.State.make_self_init ()

let random_key () =
  let alphanumeric_char _ =
    let num = List.range ~stop:`inclusive 48 57 in
    let upper = List.range ~stop:`inclusive 65 90 in
    let lower = List.range ~stop:`inclusive 97 122 in
    let alnum = num @ upper @ lower in
    let random_int = List.random_element_exn ~random_state alnum in
    Char.of_int_exn random_int
  in
  let random_string = String.init 7 ~f:alphanumeric_char in
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

let test_get () =
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
    let expected = Ok "blurb" in
    Alcotest.(check (result some_string err)) "BGREWRITEAOF failed" expected res;
    let%bind _ = Deferred.ok @@ after (Time.Span.of_sec 1.) in
    return ()

let test_bgsave () =
  Orewa.connect ~host @@ fun conn ->
    let%bind res = Orewa.bgsave conn in
    let expected = Ok "blurb" in
    Alcotest.(check (result some_string err)) "BGSAVE failed" expected res;
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

let test_bitop () =
  Orewa.connect ~host @@ fun conn ->
    let key = random_key () in
    let destkey = random_key () in
    let value = "aaaa" in
    let expected = Ok (String.length value) in
    let%bind _ = Orewa.set conn ~key value in
    let%bind res = Orewa.bitop conn ~destkey ~keys:[key] ~key Orewa.AND in
    Alcotest.(check (result int err)) "BITOP failed" expected res;
    let%bind res = Orewa.bitop conn ~destkey ~keys:[key] ~key Orewa.XOR in
    Alcotest.(check (result int err)) "BITOP failed" expected res;
    return ()

let test_decr () =
  Orewa.connect ~host @@ fun conn ->
    let key = random_key () in
    let value = 42 in
    let%bind _ = Orewa.set conn ~key (string_of_int value) in
    let%bind res = Orewa.decr conn key in
    Alcotest.(check (result int err)) "DECR failed" (Ok (Int.pred value)) res;
    return ()

let test_decrby () =
  Orewa.connect ~host @@ fun conn ->
    let key = random_key () in
    let value = 42 in
    let decrement = 23 in
    let%bind _ = Orewa.set conn ~key (string_of_int value) in
    let%bind res = Orewa.decrby conn key decrement in
    Alcotest.(check (result int err)) "DECRBY failed" (Ok (value - decrement)) res;
    return ()

let test_incr () =
  Orewa.connect ~host @@ fun conn ->
    let key = random_key () in
    let value = 42 in
    let%bind _ = Orewa.set conn ~key (string_of_int value) in
    let%bind res = Orewa.incr conn key in
    Alcotest.(check (result int err)) "INCR failed" (Ok (Int.succ value)) res;
    return ()

let test_incrby () =
  Orewa.connect ~host @@ fun conn ->
    let key = random_key () in
    let value = 42 in
    let increment = 23 in
    let%bind _ = Orewa.set conn ~key (string_of_int value) in
    let%bind res = Orewa.incrby conn key increment in
    Alcotest.(check (result int err)) "INCRBY failed" (Ok (value + increment)) res;
    return ()

let test_select () =
  Orewa.connect ~host @@ fun conn ->
    let index = 5 in
    let%bind res = Orewa.select conn index in
    Alcotest.(check (result unit err)) "SELECT failed" (Ok ()) res;
    return ()

let test_del () =
  Orewa.connect ~host @@ fun conn ->
    let key = random_key () in
    let key' = random_key () in
    let value = "aaaa" in
    let%bind _ = Orewa.set conn ~key value in
    let%bind _ = Orewa.set conn ~key:key' value in
    let%bind res = Orewa.del conn ~keys:[key'] key in
    Alcotest.(check (result int err)) "DEL failed" (Ok 2) res;
    return ()

let test_exists () =
  Orewa.connect ~host @@ fun conn ->
    let existing = random_key () in
    let missing = random_key () in
    let value = "aaaa" in
    let%bind _ = Orewa.set conn ~key:existing value in
    let%bind res = Orewa.exists conn ~keys:[existing] missing in
    Alcotest.(check (result int err)) "EXISTS failed" (Ok 1) res;
    return ()

let test_expire () =
  Orewa.connect ~host @@ fun conn ->
    let key = random_key () in
    let value = "aaaa" in
    let expire = Time.Span.of_ms 200. in
    let%bind _ = Orewa.set conn ~key value in
    let%bind res= Orewa.expire conn key expire in
    Alcotest.(check (result int err)) "Correctly SET expiry" (Ok 1) res;
    let%bind res = Orewa.exists conn key in
    Alcotest.(check (result int err)) "Key still exists" (Ok 1) res;
    let%bind () = after Time.Span.(expire / 0.75) in
    let%bind res = Orewa.exists conn key in
    Alcotest.(check (result int err)) "Key has expired" (Ok 0) res;
    return ()

let test_expireat () =
  Orewa.connect ~host @@ fun conn ->
    let key = random_key () in
    let value = "aaaa" in
    let expire = Time.Span.of_ms 200. in
    let at = Time.add (Time.now ()) expire in
    let%bind _ = Orewa.set conn ~key value in
    let%bind res = Orewa.expireat conn key at in
    Alcotest.(check (result int err)) "Correctly SET expiry" (Ok 1) res;
    let%bind res = Orewa.exists conn key in
    Alcotest.(check (result int err)) "Key still exists" (Ok 1) res;
    let%bind () = after Time.Span.(expire / 0.75) in
    let%bind res = Orewa.exists conn key in
    Alcotest.(check (result int err)) "Key has expired" (Ok 0) res;
    return ()

let test_keys () =
  Orewa.connect ~host @@ fun conn ->
    let prefix = random_key () in
    let key1 = prefix ^ (random_key ()) in
    let key2 = prefix ^ (random_key ()) in
    let value = "aaaa" in
    let%bind _ = Orewa.set conn ~key:key1 value in
    let%bind _ = Orewa.set conn ~key:key2 value in
    let%bind res = Orewa.keys conn (prefix ^ "*") in
    Alcotest.(check (result unordered_string_list err)) "Returns the right keys" (Ok [key1; key2]) res;
    let none = random_key () in
    let%bind res = Orewa.keys conn (none ^ "*") in
    Alcotest.(check (result (list string) err)) "Returns no keys" (Ok []) res;
    return ()

let test_scan () =
  Orewa.connect ~host @@ fun conn ->
    let prefix = random_key () in
    let value = "aaaa" in
    let count = 20 in
    let%bind expected_keys = Deferred.List.map (List.range 0 count) ~f:(fun index ->
      let key = Printf.sprintf "%s:%d" prefix index in
      let%bind _ = Orewa.set conn ~key value in
      return key)
    in
    let pattern = prefix ^ "*" in
    let pipe = Orewa.scan ~pattern ~count conn in
    let%bind q = Pipe.read_all pipe in
    let res = Queue.to_list q in
    Alcotest.(check unordered_string_list)
      "Returns the right keys"
      expected_keys res;
    return ()

let test_move () =
  Orewa.connect ~host @@ fun conn ->
    let key = random_key () in
    let value = "aaaa" in
    let other_db = 4 in
    let original_db = 0 in
    let%bind _ = Orewa.select conn original_db in
    let%bind _ = Orewa.set conn ~key value in
    let%bind res = Orewa.move conn key other_db in
    Alcotest.(check (result bool err)) "Successfully moved" (Ok true) res;
    let%bind _ = Orewa.select conn other_db in
    let%bind res = Orewa.get conn key in
    Alcotest.(check soe) "Key in other db" (Ok (Some value)) res;
    let%bind _ = Orewa.select conn original_db in
    let%bind res = Orewa.move conn key other_db in
    Alcotest.(check (result bool err)) "MOVE failed as expected" (Ok false) res;
    return ()

let test_persist () =
  Orewa.connect ~host @@ fun conn ->
    let key = random_key () in
    let missing_key = random_key () in
    let value = "aaaa" in
    let%bind _ = Orewa.set conn ~expire:(Time.Span.of_sec 30.) ~key value in
    let%bind res = Orewa.persist conn key in
    Alcotest.(check (result bool err)) "Set key to persistent" (Ok true) res;
    let%bind res = Orewa.persist conn key in
    Alcotest.(check (result bool err)) "Key couldn't be persisted twice" (Ok false) res;
    let%bind res = Orewa.persist conn missing_key in
    Alcotest.(check (result bool err)) "Missing key couldn't be persisted" (Ok false) res;
    return ()

let test_randomkey () =
  Orewa.connect ~host @@ fun conn ->
    let key = random_key () in
    let value = "aaaa" in
    let%bind _ = Orewa.set conn ~key value in
    let%bind res = Orewa.randomkey conn in
    Alcotest.(check (result some_string err)) "Got random key" (Ok "anything") res;
    return ()

let test_rename () =
  Orewa.connect ~host @@ fun conn ->
    let key = random_key () in
    let new_key = random_key () in
    let value = "aaaa" in
    let%bind _ = Orewa.set conn ~key value in
    let%bind res = Orewa.rename conn key new_key in
    Alcotest.(check ue) "Successfully renamed" (Ok ()) res;
    let%bind res = Orewa.get conn new_key in
    Alcotest.(check soe) "Key exists in new location" (Ok (Some value)) res;
    let%bind res = Orewa.get conn key in
    Alcotest.(check soe) "Key gone in old location" (Ok None) res;
    return ()

let test_renamenx () =
  Orewa.connect ~host @@ fun conn ->
    let key = random_key () in
    let new_key = random_key () in
    let value = "aaaa" in
    let%bind _ = Orewa.set conn ~key value in
    let%bind res = Orewa.renamenx conn ~key new_key in
    Alcotest.(check (result bool err)) "Successfully renamed" (Ok true) res;
    let%bind _ = Orewa.set conn ~key value in
    let%bind res = Orewa.renamenx conn ~key new_key in
    Alcotest.(check (result bool err)) "Renaming to existing key shouldn't work" (Ok false) res;
    return ()

let tests = Alcotest_async.[
  test_case "ECHO" `Slow test_echo;
  test_case "SET" `Slow test_set;
  test_case "GET" `Slow test_get;
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
  test_case "BITOP" `Slow test_bitop;
  test_case "DECR" `Slow test_decr;
  test_case "DECRBY" `Slow test_decrby;
  test_case "INCR" `Slow test_incr;
  test_case "INCRBY" `Slow test_incrby;
  test_case "SELECT" `Slow test_select;
  test_case "DEL" `Slow test_del;
  test_case "EXISTS" `Slow test_exists;
  test_case "EXPIRE" `Slow test_expire;
  test_case "EXPIREAT" `Slow test_expireat;
  test_case "KEYS" `Slow test_keys;
  test_case "SCAN" `Slow test_scan;
  test_case "MOVE" `Slow test_move;
  test_case "PERSIST" `Slow test_persist;
  test_case "RANDOMKEY" `Slow test_randomkey;
  test_case "RENAME" `Slow test_rename;
  test_case "RENAMENX" `Slow test_renamenx;
]

let () =
  Log.Global.set_level `Debug;
  Alcotest.run Caml.__MODULE__ [
    ("integration", tests);
  ]
