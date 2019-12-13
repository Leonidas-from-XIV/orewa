open Core
open Async

(* This integration test will leak Redis keys left and right *)

let host = "localhost"

let exceeding_read_buffer = 128 * 1024

module Orewa_error = struct
  type t =
    [ Orewa.common_error
    | `Redis_error of string
    | `No_such_key of string
    | `Not_expiring of string
    | `Wrong_type of string
    | `Index_out_of_range of string ]
  [@@deriving show, eq]
end

let err = Alcotest.testable Orewa_error.pp Orewa_error.equal

let ue = Alcotest.(result unit err)

let be = Alcotest.(result bool err)

let ie = Alcotest.(result int err)

let se = Alcotest.(result string err)

let soe = Alcotest.(result (option string) err)

let some_string = Alcotest.testable String.pp (const (const true))

let bit = Alcotest.testable Orewa.pp_bit Orewa.equal_bit

let colon ppf _ = Fmt.pf ppf ":@, "

let dump_string ppf s = Fmt.pf ppf "%S" s

let pp_binding = Fmt.(pair ~sep:colon dump_string dump_string)

let smap_iter f m = String.Map.iteri m ~f:(fun ~key ~data -> f key data)

let string_string_map_pp = Fmt.(braces (iter_bindings ~sep:comma smap_iter pp_binding))

let sm = Alcotest.testable string_string_map_pp (String.Map.equal String.equal)

let sme = Alcotest.(result sm err)

let unordered_string_list =
  Alcotest.(
    testable
      (pp (list string))
      (fun a b ->
        let equal = equal (list string) in
        let compare = String.compare in
        equal (List.sort ~compare a) (List.sort ~compare b)))

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
  Orewa.with_connection ~host @@ fun conn ->
  let message = "Hello" in
  let%bind response = Orewa.echo conn message in
  Alcotest.(check se) "ECHO faulty" (Ok message) response;
  return ()

let test_set () =
  Orewa.with_connection ~host @@ fun conn ->
  let key = random_key () in
  let%bind res = Orewa.set conn ~key "value" in
  Alcotest.(check be) "Successfully SET" (Ok true) res;
  let%bind res = Orewa.set conn ~key ~exist:`Not_if_exists "other" in
  Alcotest.(check be) "Didn't SET again" (Ok false) res;
  let%bind res = Orewa.set conn ~key ~exist:`Only_if_exists "other" in
  Alcotest.(check be) "Successfully re-SET" (Ok true) res;
  let not_existing = random_key () in
  let%bind res = Orewa.set conn ~key:not_existing ~exist:`Only_if_exists "value" in
  Alcotest.(check be) "Didn't SET non-existing" (Ok false) res;
  return ()

let test_get () =
  Orewa.with_connection ~host @@ fun conn ->
  let key = random_key () in
  let value = random_key () in
  let%bind _ = Orewa.set conn ~key value in
  let%bind res = Orewa.get conn key in
  Alcotest.(check soe) "Correct response" (Ok (Some value)) res;
  return ()

let test_getset () =
  Orewa.with_connection ~host @@ fun conn ->
  let key = random_key () in
  let value = random_key () in
  let value' = random_key () in
  let%bind res = Orewa.getset conn ~key value in
  Alcotest.(check soe) "Setting non-existing key returns no previous value" (Ok None) res;
  let%bind res = Orewa.getset conn ~key value' in
  Alcotest.(check soe) "Setting existing key returns previous value" (Ok (Some value)) res;
  return ()

let test_strlen () =
  Orewa.with_connection ~host @@ fun conn ->
  let key = random_key () in
  let value = random_key () in
  let%bind res = Orewa.strlen conn key in
  Alcotest.(check ie) "Length of empty key is zero" (Ok 0) res;
  let%bind _ = Orewa.set conn ~key value in
  let%bind res = Orewa.strlen conn key in
  Alcotest.(check ie)
    "Length of key is determined correctly"
    (Ok (String.length value))
    res;
  return ()

let test_mget () =
  Orewa.with_connection ~host @@ fun conn ->
  let key = random_key () in
  let non_existing_key = random_key () in
  let value = random_key () in
  let%bind _ = Orewa.set conn ~key value in
  let%bind res = Orewa.mget conn [non_existing_key; key; key] in
  Alcotest.(check (result (list (option string)) err))
    "Correct response"
    (Ok [None; Some value; Some value])
    res;
  return ()

let test_msetnx () =
  Orewa.with_connection ~host @@ fun conn ->
  let key = random_key () in
  let value = random_key () in
  let key' = random_key () in
  let value' = random_key () in
  let key'' = random_key () in
  let value'' = random_key () in
  let be = Alcotest.(result bool err) in
  let%bind res = Orewa.msetnx conn [key, value; key', value'] in
  Alcotest.(check be) "Setting once succeeded" (Ok true) res;
  let%bind res = Orewa.mget conn [key; key'] in
  Alcotest.(check (result (list (option string)) err))
    "Keys as expected"
    (Ok [Some value; Some value'])
    res;
  let%bind res = Orewa.msetnx conn [key', value''; key'', value''] in
  Alcotest.(check be) "Setting once succeeded" (Ok false) res;
  let%bind res = Orewa.mget conn [key; key'; key''] in
  Alcotest.(check (result (list (option string)) err))
    "Keys as expected"
    (Ok [Some value; Some value'; None])
    res;
  return ()

let test_mset () =
  Orewa.with_connection ~host @@ fun conn ->
  let key = random_key () in
  let value = random_key () in
  let key' = random_key () in
  let value' = random_key () in
  let%bind res = Orewa.mset conn [key, value; key', value'] in
  Alcotest.(check ue) "Correct response" (Ok ()) res;
  let%bind res = Orewa.mget conn [key; key'] in
  Alcotest.(check (result (list (option string)) err))
    "Correct response"
    (Ok [Some value; Some value'])
    res;
  return ()

let test_getrange () =
  Orewa.with_connection ~host @@ fun conn ->
  let key = random_key () in
  let not_existing_key = random_key () in
  let value = "Hello" in
  let%bind _ = Orewa.set conn ~key value in
  let%bind res = Orewa.getrange conn key ~start:1 ~end':3 in
  Alcotest.(check se) "Correct response" (Ok "ell") res;
  let%bind res = Orewa.getrange conn not_existing_key ~start:1 ~end':3 in
  Alcotest.(check se) "Correct response" (Ok "") res;
  return ()

let test_set_expiry () =
  Orewa.with_connection ~host @@ fun conn ->
  let key = random_key () in
  let value = random_key () in
  let expire = Time.Span.of_ms 200. in
  let%bind res = Orewa.set conn ~key ~expire value in
  Alcotest.(check be) "Correctly SET expiry" (Ok true) res;
  let%bind res = Orewa.get conn key in
  Alcotest.(check soe) "Key still exists" (Ok (Some value)) res;
  let%bind () = after Time.Span.(expire / 0.75) in
  let%bind res = Orewa.get conn key in
  Alcotest.(check soe) "Key has expired" (Ok None) res;
  return ()

let test_large_set_get () =
  Orewa.with_connection ~host @@ fun conn ->
  let key = random_key () in
  let value = String.init exceeding_read_buffer ~f:(fun _ -> 'a') in
  let%bind res = Orewa.set conn ~key value in
  Alcotest.(check be) "Large SET failed" (Ok true) res;
  let%bind res = Orewa.get conn key in
  Alcotest.(check soe) "Large GET retrieves everything" (Ok (Some value)) res;
  return ()

let test_lpush () =
  Orewa.with_connection ~host @@ fun conn ->
  let key = random_key () in
  let not_list = random_key () in
  let element = "value" in
  let%bind res = Orewa.lpush conn ~exist:`Only_if_exists ~element key in
  Alcotest.(check ie) "LPUSHX to non-existing list" (Ok 0) res;
  let%bind res = Orewa.lpush conn ~element key in
  Alcotest.(check ie) "LPUSH to empty list" (Ok 1) res;
  let%bind res = Orewa.lpush conn ~exist:`Always ~element key in
  Alcotest.(check ie) "LPUSH to existing list" (Ok 2) res;
  let%bind _ = Orewa.set conn ~key:not_list element in
  let%bind res = Orewa.lpush conn ~element not_list in
  Alcotest.(check ie) "LPUSH to not a list" (Error (`Wrong_type not_list)) res;
  return ()

let test_rpush () =
  Orewa.with_connection ~host @@ fun conn ->
  let key = random_key () in
  let not_list = random_key () in
  let element = "value" in
  let%bind res = Orewa.rpush conn ~exist:`Only_if_exists ~element key in
  Alcotest.(check ie) "RPUSHX to non-existing list" (Ok 0) res;
  let%bind res = Orewa.rpush conn ~element key in
  Alcotest.(check ie) "RPUSH to empty list" (Ok 1) res;
  let%bind res = Orewa.rpush conn ~exist:`Always ~element key in
  Alcotest.(check ie) "RPUSH to existing list" (Ok 2) res;
  let%bind _ = Orewa.set conn ~key:not_list element in
  let%bind res = Orewa.rpush conn ~element not_list in
  Alcotest.(check ie) "RPUSH to not a list" (Error (`Wrong_type not_list)) res;
  return ()

let test_lpush_lrange () =
  Orewa.with_connection ~host @@ fun conn ->
  let key = random_key () in
  let element = random_key () in
  let element' = random_key () in
  let%bind _ = Orewa.lpush conn ~element key in
  let%bind _ = Orewa.lpush conn ~element:element' key in
  let%bind res = Orewa.lrange conn ~key ~start:0 ~stop:(-1) in
  Alcotest.(check (result (list truncated_string) err))
    "LRANGE failed"
    (Ok [element'; element])
    res;
  return ()

let test_large_lrange () =
  Orewa.with_connection ~host @@ fun conn ->
  let key = random_key () in
  let element = String.init exceeding_read_buffer ~f:(fun _ -> 'a') in
  let values = 5 in
  let%bind expected =
    Deferred.List.init values ~f:(fun _ ->
        let%bind _ = Orewa.lpush conn ~element key in
        return element)
  in
  let%bind res = Orewa.lrange conn ~key ~start:0 ~stop:(-1) in
  Alcotest.(check (result (list truncated_string) err)) "LRANGE failed" (Ok expected) res;
  return ()

let test_rpoplpush () =
  Orewa.with_connection ~host @@ fun conn ->
  let source = random_key () in
  let destination = random_key () in
  let element = "three" in
  let not_list = random_key () in
  let%bind _ = Orewa.rpush conn source ~element:"one" in
  let%bind _ = Orewa.rpush conn source ~element:"two" in
  let%bind _ = Orewa.rpush conn source ~element in
  let%bind res = Orewa.rpoplpush conn ~source ~destination in
  Alcotest.(check se) "RPOPLPUSH moved the correct element" (Ok element) res;
  let%bind _ = Orewa.set conn ~key:not_list element in
  let%bind res = Orewa.rpoplpush conn ~source ~destination:not_list in
  let wrong_move_destination = Printf.sprintf "%s -> %s" source not_list in
  Alcotest.(check se)
    "RPOPLPUSH failed to move to non-list"
    (Error (`Wrong_type wrong_move_destination))
    res;
  let%bind res = Orewa.rpoplpush conn ~source:not_list ~destination in
  let wrong_move_source = Printf.sprintf "%s -> %s" not_list destination in
  Alcotest.(check se)
    "RPOPLPUSH failed to move from non-list"
    (Error (`Wrong_type wrong_move_source))
    res;
  return ()

let test_append () =
  Orewa.with_connection ~host @@ fun conn ->
  let key = random_key () in
  let value = random_key () in
  let%bind res = Orewa.append conn ~key value in
  Alcotest.(check (result int err)) "APPEND unexpected" (Ok (String.length value)) res;
  return ()

let test_auth () =
  Orewa.with_connection ~host @@ fun conn ->
  let password = random_key () in
  let%bind res = Orewa.auth conn password in
  let expected = Error (`Redis_error "ERR Client sent AUTH, but no password is set") in
  Alcotest.(check (result unit err)) "AUTH failed" expected res;
  return ()

let test_bgrewriteaof () =
  Orewa.with_connection ~host @@ fun conn ->
  let%bind res = Orewa.bgrewriteaof conn in
  let expected = Ok "blurb" in
  Alcotest.(check (result some_string err)) "BGREWRITEAOF failed" expected res;
  let%bind _ = Deferred.ok @@ after (Time.Span.of_sec 1.) in
  return ()

let test_bgsave () =
  Orewa.with_connection ~host @@ fun conn ->
  let%bind res = Orewa.bgsave conn in
  let expected = Ok "blurb" in
  Alcotest.(check (result some_string err)) "BGSAVE failed" expected res;
  return ()

let test_bitcount () =
  Orewa.with_connection ~host @@ fun conn ->
  let key = random_key () in
  let%bind _ = Orewa.set conn ~key "aaaa" in
  let%bind res = Orewa.bitcount conn key in
  Alcotest.(check (result int err)) "BITCOUNT failed" (Ok 12) res;
  let%bind res = Orewa.bitcount conn ~range:(1, 2) key in
  Alcotest.(check (result int err)) "BITCOUNT failed" (Ok 6) res;
  return ()

let test_bitop () =
  Orewa.with_connection ~host @@ fun conn ->
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

let test_bitpos () =
  Orewa.with_connection ~host @@ fun conn ->
  let key = random_key () in
  let value = "\000\001\000\000\001" in
  let expected = Ok (Some 15) in
  let%bind _ = Orewa.set conn ~key value in
  let%bind res = Orewa.bitpos conn key One in
  Alcotest.(check (result (option int) err)) "BITPOS failed" expected res;
  let%bind res = Orewa.bitpos conn ~start:2 key One in
  let expected = Ok (Some 39) in
  Alcotest.(check (result (option int) err)) "BITPOS failed" expected res;
  let%bind res = Orewa.bitpos conn ~start:2 ~end':3 key One in
  let expected = Ok None in
  Alcotest.(check (result (option int) err)) "BITPOS failed" expected res;
  return ()

let test_getbit () =
  Orewa.with_connection ~host @@ fun conn ->
  let key = random_key () in
  let value = "\001" in
  let%bind _ = Orewa.set conn ~key value in
  let expected = Ok Orewa.Zero in
  let%bind res = Orewa.getbit conn key 0 in
  Alcotest.(check (result bit err)) "GETBIT failed" expected res;
  let expected = Ok Orewa.Zero in
  let%bind res = Orewa.getbit conn key 8 in
  Alcotest.(check (result bit err)) "GETBIT failed" expected res;
  return ()

let test_setbit () =
  Orewa.with_connection ~host @@ fun conn ->
  let key = random_key () in
  let offset = 10 in
  let%bind res = Orewa.setbit conn key offset Orewa.One in
  Alcotest.(check (result bit err)) "SETBIT failed" (Ok Orewa.Zero) res;
  let%bind res = Orewa.setbit conn key offset Orewa.Zero in
  Alcotest.(check (result bit err)) "SETBIT failed" (Ok Orewa.One) res;
  return ()

let test_bitfield () =
  Orewa.with_connection ~host @@ fun conn ->
  let key = random_key () in
  let ile = Alcotest.(result (list (option int)) err) in
  let intsize = Orewa.Unsigned 8 in
  let maxsize = 255 in
  let%bind res = Orewa.bitfield conn key [Set (intsize, Absolute 0, 1)] in
  Alcotest.(check ile) "Setting returns previous value" (Ok [Some 0]) res;
  let%bind res = Orewa.bitfield conn key [Set (intsize, Absolute 0, 0)] in
  Alcotest.(check ile) "Setting returns previous value" (Ok [Some 1]) res;
  let%bind res = Orewa.bitfield conn key [Get (intsize, Absolute 0)] in
  Alcotest.(check ile) "Getting returns current value" (Ok [Some 0]) res;
  let overflow_by = 42 in
  let%bind res =
    Orewa.bitfield
      conn
      key
      ~overflow:Orewa.Wrap
      [ Set (intsize, Relative 0, maxsize);
        Incrby (intsize, Relative 0, Int.succ overflow_by) ]
  in
  Alcotest.(check ile)
    "Relative setting and overflow work"
    (Ok [Some 0; Some overflow_by])
    res;
  let%bind res =
    Orewa.bitfield
      conn
      key
      ~overflow:Orewa.Sat
      [Set (intsize, Relative 0, maxsize); Incrby (intsize, Relative 0, 1)]
  in
  Alcotest.(check ile)
    "Saturated overflow works"
    (Ok [Some overflow_by; Some maxsize])
    res;
  let%bind res =
    Orewa.bitfield conn key ~overflow:Orewa.Fail [Incrby (intsize, Relative 0, 1)]
  in
  Alcotest.(check ile) "Failing overflow works" (Ok [None]) res;
  return ()

let test_decr () =
  Orewa.with_connection ~host @@ fun conn ->
  let key = random_key () in
  let value = 42 in
  let%bind _ = Orewa.set conn ~key (string_of_int value) in
  let%bind res = Orewa.decr conn key in
  Alcotest.(check (result int err)) "DECR failed" (Ok (Int.pred value)) res;
  return ()

let test_decrby () =
  Orewa.with_connection ~host @@ fun conn ->
  let key = random_key () in
  let value = 42 in
  let decrement = 23 in
  let%bind _ = Orewa.set conn ~key (string_of_int value) in
  let%bind res = Orewa.decrby conn key decrement in
  Alcotest.(check (result int err)) "DECRBY failed" (Ok (value - decrement)) res;
  return ()

let test_incr () =
  Orewa.with_connection ~host @@ fun conn ->
  let key = random_key () in
  let value = 42 in
  let%bind _ = Orewa.set conn ~key (string_of_int value) in
  let%bind res = Orewa.incr conn key in
  Alcotest.(check (result int err)) "INCR failed" (Ok (Int.succ value)) res;
  return ()

let test_incrby () =
  Orewa.with_connection ~host @@ fun conn ->
  let key = random_key () in
  let value = 42 in
  let increment = 23 in
  let%bind _ = Orewa.set conn ~key (string_of_int value) in
  let%bind res = Orewa.incrby conn key increment in
  Alcotest.(check (result int err)) "INCRBY failed" (Ok (value + increment)) res;
  return ()

let test_incrbyfloat () =
  Orewa.with_connection ~host @@ fun conn ->
  let key = random_key () in
  let increment = 42. in
  let%bind res = Orewa.incrbyfloat conn key increment in
  Alcotest.(check (result (float 0.1) err)) "INCRBYFLOAT failed" (Ok increment) res;
  return ()

let test_select () =
  Orewa.with_connection ~host @@ fun conn ->
  let index = 5 in
  let%bind res = Orewa.select conn index in
  Alcotest.(check (result unit err)) "SELECT failed" (Ok ()) res;
  return ()

let test_del () =
  Orewa.with_connection ~host @@ fun conn ->
  let key = random_key () in
  let key' = random_key () in
  let value = "aaaa" in
  let%bind _ = Orewa.set conn ~key value in
  let%bind _ = Orewa.set conn ~key:key' value in
  let%bind res = Orewa.del conn ~keys:[key'] key in
  Alcotest.(check (result int err)) "DEL failed" (Ok 2) res;
  return ()

let test_exists () =
  Orewa.with_connection ~host @@ fun conn ->
  let existing = random_key () in
  let missing = random_key () in
  let value = "aaaa" in
  let%bind _ = Orewa.set conn ~key:existing value in
  let%bind res = Orewa.exists conn ~keys:[existing] missing in
  Alcotest.(check (result int err)) "EXISTS failed" (Ok 1) res;
  return ()

let test_expire () =
  Orewa.with_connection ~host @@ fun conn ->
  let key = random_key () in
  let value = "aaaa" in
  let expire = Time.Span.of_ms 200. in
  let%bind _ = Orewa.set conn ~key value in
  let%bind res = Orewa.expire conn key expire in
  Alcotest.(check (result int err)) "Correctly SET expiry" (Ok 1) res;
  let%bind res = Orewa.exists conn key in
  Alcotest.(check (result int err)) "Key still exists" (Ok 1) res;
  let%bind () = after Time.Span.(expire / 0.75) in
  let%bind res = Orewa.exists conn key in
  Alcotest.(check (result int err)) "Key has expired" (Ok 0) res;
  return ()

let test_expireat () =
  Orewa.with_connection ~host @@ fun conn ->
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
  Orewa.with_connection ~host @@ fun conn ->
  let prefix = random_key () in
  let key1 = prefix ^ random_key () in
  let key2 = prefix ^ random_key () in
  let value = "aaaa" in
  let%bind _ = Orewa.set conn ~key:key1 value in
  let%bind _ = Orewa.set conn ~key:key2 value in
  let%bind res = Orewa.keys conn (prefix ^ "*") in
  Alcotest.(check (result unordered_string_list err))
    "Returns the right keys"
    (Ok [key1; key2])
    res;
  let none = random_key () in
  let%bind res = Orewa.keys conn (none ^ "*") in
  Alcotest.(check (result (list string) err)) "Returns no keys" (Ok []) res;
  return ()

let test_sadd () =
  Orewa.with_connection ~host @@ fun conn ->
  let key = random_key () in
  let zero = "0" in
  let dup = "dup" in
  let%bind res = Orewa.sadd conn ~key zero in
  Alcotest.(check (result int err)) "Inserts single value" (Ok 1) res;
  let%bind res = Orewa.sadd conn ~key "a" ~members:["b"; "c"] in
  Alcotest.(check (result int err)) "Inserts multiple values" (Ok 3) res;
  let%bind res = Orewa.sadd conn ~key zero in
  Alcotest.(check (result int err)) "Skips single duplicate value" (Ok 0) res;
  let%bind res = Orewa.sadd conn ~key dup ~members:[zero; dup] in
  Alcotest.(check (result int err)) "Skips multiple duplicate value" (Ok 1) res;
  return ()

let test_scard () =
  Orewa.with_connection ~host @@ fun conn ->
  let key = random_key () in
  let dup = "dup" in
  let%bind res = Orewa.scard conn key in
  Alcotest.(check (result int err)) "Nonexistant key is empty" (Ok 0) res;
  let%bind _ = Orewa.sadd conn ~key dup in
  let%bind res = Orewa.scard conn key in
  Alcotest.(check (result int err)) "Existent set has one member" (Ok 1) res;
  let%bind _ = Orewa.sadd conn ~key "a" ~members:["b"; "c"] in
  let%bind res = Orewa.scard conn key in
  Alcotest.(check (result int err)) "New set has even more members" (Ok 4) res;
  return ()

let test_sdiff () =
  Orewa.with_connection ~host @@ fun conn ->
  let key1 = random_key () in
  let key2 = random_key () in
  let%bind _ = Orewa.sadd conn ~key:key1 "a" ~members:["b"; "c"] in
  let%bind _ = Orewa.sadd conn ~key:key2 "c" ~members:["d"; "e"] in
  let%bind res = Orewa.sdiff conn key1 ~keys:[key2] in
  Alcotest.(check (result unordered_string_list err))
    "Correct differing set"
    (Ok ["a"; "b"])
    res;
  return ()

let test_sdiffstore () =
  Orewa.with_connection ~host @@ fun conn ->
  let key1 = random_key () in
  let key2 = random_key () in
  let destination = random_key () in
  let%bind _ = Orewa.sadd conn ~key:key1 "a" ~members:["b"; "c"] in
  let%bind _ = Orewa.sadd conn ~key:key2 "c" ~members:["d"; "e"] in
  let%bind res = Orewa.sdiffstore conn ~destination ~key:key1 ~keys:[key2] in
  Alcotest.(check (result int err)) "New set the right amount of members" (Ok 2) res;
  return ()

let test_sinter () =
  Orewa.with_connection ~host @@ fun conn ->
  let key1 = random_key () in
  let key2 = random_key () in
  let%bind _ = Orewa.sadd conn ~key:key1 "a" ~members:["b"; "c"] in
  let%bind _ = Orewa.sadd conn ~key:key2 "c" ~members:["d"; "e"] in
  let%bind res = Orewa.sinter conn key1 ~keys:[key2] in
  Alcotest.(check (result unordered_string_list err))
    "Correct differing set"
    (Ok ["c"])
    res;
  return ()

let test_sinterstore () =
  Orewa.with_connection ~host @@ fun conn ->
  let key1 = random_key () in
  let key2 = random_key () in
  let destination = random_key () in
  let%bind _ = Orewa.sadd conn ~key:key1 "a" ~members:["b"; "c"] in
  let%bind _ = Orewa.sadd conn ~key:key2 "c" ~members:["d"; "e"] in
  let%bind res = Orewa.sinterstore conn ~destination ~key:key1 ~keys:[key2] in
  Alcotest.(check (result int err))
    "The right amount of members was calculated"
    (Ok 1)
    res;
  let%bind res = Orewa.scard conn destination in
  Alcotest.(check (result int err)) "The right members are in the new set" (Ok 1) res;
  return ()

let test_sismember () =
  Orewa.with_connection ~host @@ fun conn ->
  let key = random_key () in
  let member = "aaa" in
  let not_member = "bbb" in
  let%bind res = Orewa.sismember conn ~key member in
  Alcotest.(check (result bool err)) "Not member in inexistent set" (Ok false) res;
  let%bind _ = Orewa.sadd conn ~key member in
  let%bind res = Orewa.sismember conn ~key member in
  Alcotest.(check (result bool err)) "Member in set" (Ok true) res;
  let%bind res = Orewa.sismember conn ~key not_member in
  Alcotest.(check (result bool err)) "Not member in set" (Ok false) res;
  return ()

let test_smembers () =
  Orewa.with_connection ~host @@ fun conn ->
  let key = random_key () in
  let member = "aaa" in
  let%bind res = Orewa.smembers conn key in
  Alcotest.(check (result (list string) err)) "Not member in inexistent set" (Ok []) res;
  let%bind _ = Orewa.sadd conn ~key member in
  let%bind res = Orewa.smembers conn key in
  Alcotest.(check (result (list string) err)) "Members in existent set" (Ok [member]) res;
  return ()

let test_smove () =
  Orewa.with_connection ~host @@ fun conn ->
  let source = random_key () in
  let destination = random_key () in
  let member = "aaa" in
  let%bind res = Orewa.smove conn ~source ~destination member in
  Alcotest.(check (result bool err))
    "Moving from a set where not member is noop"
    (Ok false)
    res;
  let%bind _ = Orewa.sadd conn ~key:source member in
  let%bind res = Orewa.smove conn ~source ~destination member in
  Alcotest.(check (result bool err)) "Moving from a set works" (Ok false) res;
  let%bind res = Orewa.sismember conn ~key:source member in
  Alcotest.(check (result bool err)) "Correctly removed from source" (Ok false) res;
  let%bind res = Orewa.sismember conn ~key:destination member in
  Alcotest.(check (result bool err)) "Correctly arrived in destination" (Ok true) res;
  return ()

let test_spop () =
  Orewa.with_connection ~host @@ fun conn ->
  let key = random_key () in
  let%bind res = Orewa.spop conn key in
  Alcotest.(check (result (list string) err)) "Popping from nonexistent set" (Ok []) res;
  let%bind _ = Orewa.sadd conn ~key "a" ~members:["b"; "c"] in
  let%bind res = Orewa.spop conn key in
  let length = Result.map ~f:List.length in
  Alcotest.(check (result int err)) "Popping one from existing set" (Ok 1) (length res);
  let count = 2 in
  let%bind res = Orewa.spop conn ~count key in
  Alcotest.(check (result int err))
    "Popping rest from existing set"
    (Ok count)
    (length res);
  let%bind res = Orewa.scard conn key in
  Alcotest.(check (result int err)) "Set is empty now" (Ok 0) res;
  return ()

let test_srandmember () =
  Orewa.with_connection ~host @@ fun conn ->
  let key = random_key () in
  let%bind res = Orewa.srandmember conn key in
  Alcotest.(check (result (list string) err))
    "Random element from nonexistent set"
    (Ok [])
    res;
  let%bind _ = Orewa.sadd conn ~key "a" ~members:["b"; "c"] in
  let%bind res = Orewa.srandmember conn key in
  let length = Result.map ~f:List.length in
  Alcotest.(check (result int err))
    "One random member from existing set"
    (Ok 1)
    (length res);
  let count = 4 in
  let%bind res = Orewa.srandmember conn ~count key in
  Alcotest.(check (result int err))
    "Getting all elements from existing set"
    (Ok 3)
    (length res);
  return ()

let test_srem () =
  Orewa.with_connection ~host @@ fun conn ->
  let key = random_key () in
  let members = ["b"; "c"; "d"] in
  let%bind _ = Orewa.sadd conn ~key "a" ~members in
  let%bind res = Orewa.srem conn ~key "a" in
  Alcotest.(check (result int err)) "Remove single member" (Ok 1) res;
  let%bind res = Orewa.srem conn ~key "a" ~members in
  Alcotest.(check (result int err)) "Remove remaining members" (Ok 3) res;
  let%bind res = Orewa.scard conn key in
  Alcotest.(check (result int err)) "Set is empty now" (Ok 0) res;
  return ()

let test_sunion () =
  Orewa.with_connection ~host @@ fun conn ->
  let key1 = random_key () in
  let key2 = random_key () in
  let%bind _ = Orewa.sadd conn ~key:key1 "a" ~members:["b"; "c"] in
  let%bind _ = Orewa.sadd conn ~key:key2 "c" ~members:["d"; "e"] in
  let%bind res = Orewa.sunion conn key1 ~keys:[key2] in
  Alcotest.(check (result unordered_string_list err))
    "Correct differing set"
    (Ok ["a"; "b"; "c"; "d"; "e"])
    res;
  return ()

let test_sunionstore () =
  Orewa.with_connection ~host @@ fun conn ->
  let key1 = random_key () in
  let key2 = random_key () in
  let destination = random_key () in
  let%bind _ = Orewa.sadd conn ~key:key1 "a" ~members:["b"; "c"] in
  let%bind _ = Orewa.sadd conn ~key:key2 "c" ~members:["d"; "e"] in
  let%bind res = Orewa.sunionstore conn ~destination ~key:key1 ~keys:[key2] in
  Alcotest.(check (result int err))
    "The right amount of members was calculated"
    (Ok 5)
    res;
  let%bind res = Orewa.scard conn destination in
  Alcotest.(check (result int err)) "The right members are in the new set" (Ok 5) res;
  return ()

let test_sscan () =
  Orewa.with_connection ~host @@ fun conn ->
  let key = random_key () in
  print_endline key;
  let count = 20 in
  let members =
    List.init count ~f:(fun i -> String.concat ~sep:":" ["mem"; string_of_int i])
  in
  let%bind _ = Orewa.sadd conn ~key "dummy" ~members in
  let pattern = "mem:*" in
  let pipe = Orewa.sscan conn ~pattern ~count:4 key in
  let%bind q = Pipe.read_all pipe in
  let res = Queue.to_list q in
  Alcotest.(check unordered_string_list) "Returns the right keys" members res;
  return ()

let test_scan () =
  Orewa.with_connection ~host @@ fun conn ->
  let prefix = random_key () in
  let value = "aaaa" in
  let count = 20 in
  let%bind expected_keys =
    Deferred.List.map (List.range 0 count) ~f:(fun index ->
        let key = Printf.sprintf "%s:%d" prefix index in
        let%bind _ = Orewa.set conn ~key value in
        return key)
  in
  let pattern = prefix ^ "*" in
  let pipe = Orewa.scan ~pattern ~count:4 conn in
  let%bind q = Pipe.read_all pipe in
  let res = Queue.to_list q in
  Alcotest.(check unordered_string_list) "Returns the right keys" expected_keys res;
  return ()

let test_move () =
  Orewa.with_connection ~host @@ fun conn ->
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
  Orewa.with_connection ~host @@ fun conn ->
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
  Orewa.with_connection ~host @@ fun conn ->
  let key = random_key () in
  let value = "aaaa" in
  let%bind _ = Orewa.set conn ~key value in
  let%bind res = Orewa.randomkey conn in
  Alcotest.(check (result some_string err)) "Got random key" (Ok "anything") res;
  return ()

let test_rename () =
  Orewa.with_connection ~host @@ fun conn ->
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
  Orewa.with_connection ~host @@ fun conn ->
  let key = random_key () in
  let new_key = random_key () in
  let value = "aaaa" in
  let%bind _ = Orewa.set conn ~key value in
  let%bind res = Orewa.renamenx conn ~key new_key in
  Alcotest.(check (result bool err)) "Successfully renamed" (Ok true) res;
  let%bind _ = Orewa.set conn ~key value in
  let%bind res = Orewa.renamenx conn ~key new_key in
  Alcotest.(check (result bool err))
    "Renaming to existing key shouldn't work"
    (Ok false)
    res;
  return ()

type sort_result =
  [ `Count of int
  | `Sorted of string list ]
[@@deriving show, eq]

let test_sort () =
  Orewa.with_connection ~host @@ fun conn ->
  let key = random_key () in
  let randomly_ordered =
    List.range 0 10 |> List.map ~f:(fun _ -> Random.State.int random_state 1000)
  in
  let%bind () =
    Deferred.List.iter randomly_ordered ~f:(fun value ->
        let%bind _ = Orewa.lpush conn ~element:(string_of_int value) key in
        return ())
  in
  let%bind res = Orewa.sort conn key in
  let sort_result = Alcotest.testable pp_sort_result equal_sort_result in
  let integer_sorted =
    randomly_ordered |> List.sort ~compare:Int.compare |> List.map ~f:string_of_int
  in
  Alcotest.(check (result sort_result err))
    "Sorted by integer value"
    (Ok (`Sorted integer_sorted))
    res;
  let%bind res = Orewa.sort conn ~alpha:true key in
  let alpha_sorted =
    randomly_ordered |> List.map ~f:string_of_int |> List.sort ~compare:String.compare
  in
  Alcotest.(check (result sort_result err))
    "Sorted alphabetically"
    (Ok (`Sorted alpha_sorted))
    res;
  let store = random_key () in
  let%bind res = Orewa.sort conn ~store key in
  Alcotest.(check (result sort_result err))
    "Sorted all elements"
    (Ok (`Count (List.length randomly_ordered)))
    res;
  let%bind res = Orewa.lrange conn ~key:store ~start:0 ~stop:(-1) in
  Alcotest.(check (result (list string) err))
    "Sorted correctly in extra key"
    (Ok integer_sorted)
    res;
  return ()

let test_ttl () =
  Orewa.with_connection ~host @@ fun conn ->
  let key = random_key () in
  let missing_key = random_key () in
  let persistent_key = random_key () in
  let%bind res = Orewa.ttl conn missing_key in
  let span = Alcotest.testable Time.Span.pp Time.Span.equal in
  Alcotest.(check (result span err))
    "No TTL on missing keys"
    (Error (`No_such_key missing_key))
    res;
  let%bind _ = Orewa.set conn ~key:persistent_key "aaaa" in
  let%bind res = Orewa.ttl conn persistent_key in
  Alcotest.(check (result span err))
    "No TTL on persistent key"
    (Error (`Not_expiring persistent_key))
    res;
  let expire = Time.Span.of_ms 200. in
  let%bind _ = Orewa.set conn ~expire ~key "aaaa" in
  let subspan =
    Alcotest.testable Time.Span.pp (fun a b ->
        Time.Span.(a <= expire) && Time.Span.(b <= expire))
  in
  let%bind res = Orewa.ttl conn key in
  Alcotest.(check (result subspan err)) "TTL not larger than before" (Ok expire) res;
  return ()

let test_type' () =
  Orewa.with_connection ~host @@ fun conn ->
  let string_key = random_key () in
  let list_key = random_key () in
  let missing_key = random_key () in
  let%bind _ = Orewa.set conn ~key:string_key "aaaa" in
  let%bind _ = Orewa.lpush conn ~element:"aaaa" list_key in
  let%bind res = Orewa.type' conn string_key in
  Alcotest.(check soe) "Finds string" (Ok (Some "string")) res;
  let%bind res = Orewa.type' conn list_key in
  Alcotest.(check soe) "Finds list" (Ok (Some "list")) res;
  let%bind res = Orewa.type' conn missing_key in
  Alcotest.(check soe) "No hits" (Ok None) res;
  return ()

let test_dump () =
  Orewa.with_connection ~host @@ fun conn ->
  let key = random_key () in
  let missing_key = random_key () in
  let%bind _ = Orewa.set conn ~key "aaaa" in
  let%bind res = Orewa.dump conn key in
  let dump_result = Alcotest.(result (option some_string) err) in
  Alcotest.(check dump_result) "Dumping string key" (Ok (Some "anything")) res;
  let%bind res = Orewa.dump conn missing_key in
  Alcotest.(check dump_result) "Dumping missing key" (Ok None) res;
  return ()

let test_restore () =
  Orewa.with_connection ~host @@ fun conn ->
  let key = random_key () in
  let list_key = random_key () in
  let new_key = random_key () in
  let element = random_key () in
  let%bind _ = Orewa.set conn ~key element in
  let%bind res = Orewa.dump conn key in
  let dumped = Option.value_exn (Option.value_exn (Result.ok res)) in
  let%bind res = Orewa.restore conn ~key:new_key dumped in
  Alcotest.(check ue) "Restoring key" (Ok ()) res;
  let%bind res = Orewa.get conn new_key in
  Alcotest.(check soe) "Correct value restored" (Ok (Some element)) res;
  let%bind _ = Orewa.lpush conn ~element list_key in
  let%bind res = Orewa.dump conn list_key in
  let dumped = Option.value_exn (Option.value_exn (Result.ok res)) in
  let%bind res = Orewa.restore conn ~key:new_key ~replace:true dumped in
  Alcotest.(check ue) "Restoring key" (Ok ()) res;
  let%bind res = Orewa.lrange conn ~key:new_key ~start:0 ~stop:(-1) in
  Alcotest.(check (result (list string) err)) "Correct value restored" (Ok [element]) res;
  return ()

let test_pipelining () =
  Orewa.with_connection ~host @@ fun conn ->
  (* Test that we in parallel can do multiple requests *)
  let prefix = random_key () in
  let key i = Printf.sprintf "%s.%d" prefix i in
  let keys = Array.init 1000 ~f:key in
  (* Now insert all the keys *)
  let%bind () =
    Deferred.Array.iteri ~how:`Sequential keys ~f:(fun i key ->
        let%bind res = Orewa.set conn ~key (string_of_int i) in
        Alcotest.(check be) "Set test key" (Ok true) res;
        return ())
  in
  let%bind () =
    Deferred.Array.iteri
      ~how:`Parallel
      ~f:(fun i key ->
        let%bind res = Orewa.get conn key in
        Alcotest.(check soe) "Wrong value for key" (Ok (Some (string_of_int i))) res;
        return ())
      keys
  in
  return ()

let test_close () =
  let%bind conn = Orewa.connect ?port:None ~host in
  let key = random_key () in
  let%bind res = Orewa.set conn ~key "test" in
  Alcotest.(check be) "Set test key" (Ok true) res;
  let%bind res = Orewa.get conn key in
  Alcotest.(check soe) "Get test key" (Ok (Some "test")) res;
  let%bind () = Orewa.close conn in
  let%bind res = Orewa.get conn key in
  Alcotest.(check soe) "Get test key" (Error `Connection_closed) res;
  return ()

let test_lindex () =
  Orewa.with_connection ~host @@ fun conn ->
  let key = random_key () in
  let element = random_key () in
  let%bind res = Orewa.lindex conn key 0 in
  Alcotest.(check soe) "Get element out of empty list" (Ok None) res;
  let not_list = random_key () in
  let%bind _ = Orewa.set conn ~key:not_list "this is not a list" in
  let%bind res = Orewa.lindex conn key 0 in
  Alcotest.(check soe) "Get first element of not a list" (Ok None) res;
  let%bind _ = Orewa.lpush conn ~element key in
  let%bind _ = Orewa.lpush conn ~element:(random_key ()) key in
  let%bind res = Orewa.lindex conn key 1 in
  Alcotest.(check soe) "Get second element of non-empty list" (Ok (Some element)) res;
  return ()

let test_linsert () =
  Orewa.with_connection ~host @@ fun conn ->
  let key = random_key () in
  let element = random_key () in
  let pivot = random_key () in
  let%bind res = Orewa.linsert conn ~key Orewa.Before ~element ~pivot in
  Alcotest.(check ie) "Insert into nonexisting list" (Ok 0) res;
  let%bind _ = Orewa.lpush conn ~element:pivot key in
  let%bind res = Orewa.linsert conn ~key Orewa.Before ~element ~pivot in
  Alcotest.(check ie) "Insert before into existing list" (Ok 2) res;
  let%bind res = Orewa.linsert conn ~key Orewa.After ~element ~pivot in
  Alcotest.(check ie) "Insert after into existing list" (Ok 3) res;
  return ()

let test_llen () =
  Orewa.with_connection ~host @@ fun conn ->
  let key = random_key () in
  let element = random_key () in
  let%bind res = Orewa.llen conn key in
  Alcotest.(check ie) "Lenght of nonexisting list" (Ok 0) res;
  let%bind _ = Orewa.lpush conn ~element key in
  let%bind res = Orewa.llen conn key in
  Alcotest.(check ie) "Lenght of existing list" (Ok 1) res;
  return ()

let test_lpop () =
  Orewa.with_connection ~host @@ fun conn ->
  let key = random_key () in
  let not_list = random_key () in
  let element = random_key () in
  let left_element = random_key () in
  let%bind res = Orewa.lpop conn key in
  Alcotest.(check soe) "Pop from empty key" (Ok None) res;
  let%bind _ = Orewa.set conn ~key:not_list "this is not a list" in
  let%bind res = Orewa.lpop conn not_list in
  Alcotest.(check soe) "Pop from not a list" (Error (`Wrong_type not_list)) res;
  let%bind _ = Orewa.lpush conn ~element key in
  let%bind _ = Orewa.lpush conn ~element:left_element key in
  let%bind res = Orewa.lpop conn key in
  Alcotest.(check soe) "Pop from existing list" (Ok (Some left_element)) res;
  return ()

let test_rpop () =
  Orewa.with_connection ~host @@ fun conn ->
  let key = random_key () in
  let not_list = random_key () in
  let element = random_key () in
  let right_element = random_key () in
  let%bind res = Orewa.rpop conn key in
  Alcotest.(check soe) "Pop from empty key" (Ok None) res;
  let%bind _ = Orewa.set conn ~key:not_list "this is not a list" in
  let%bind res = Orewa.rpop conn not_list in
  Alcotest.(check soe) "Pop from not a list" (Error (`Wrong_type not_list)) res;
  let%bind _ = Orewa.lpush conn ~element:right_element key in
  let%bind _ = Orewa.lpush conn ~element key in
  let%bind res = Orewa.rpop conn key in
  Alcotest.(check soe) "Pop from existing list" (Ok (Some right_element)) res;
  return ()

let test_lrem () =
  Orewa.with_connection ~host @@ fun conn ->
  let key = random_key () in
  let element = random_key () in
  let%bind _ = Orewa.lpush conn key ~element in
  let%bind _ = Orewa.lpush conn key ~element in
  let%bind _ = Orewa.lpush conn key ~element:"SPACER" in
  let%bind _ = Orewa.lpush conn key ~element in
  let%bind _ = Orewa.lpush conn key ~element in
  let%bind res = Orewa.lrem conn ~key 1 ~element in
  Alcotest.(check ie) "Removing first occurence" (Ok 1) res;
  let%bind res = Orewa.lrem conn ~key 1 ~element in
  Alcotest.(check ie) "Removing second occurence" (Ok 1) res;
  let%bind res = Orewa.lrem conn ~key (-2) ~element in
  Alcotest.(check ie) "Removing final two occurence" (Ok 2) res;
  let%bind res = Orewa.llen conn key in
  Alcotest.(check ie) "Removing final two occurence" (Ok 1) res;
  let%bind res = Orewa.lrem conn ~key 1 ~element in
  Alcotest.(check ie) "Trying to remove not existing element" (Ok 0) res;
  return ()

let test_lset () =
  Orewa.with_connection ~host @@ fun conn ->
  let key = random_key () in
  let element = random_key () in
  let%bind res = Orewa.lset conn ~key 0 ~element in
  Alcotest.(check ue) "Setting nonexistent list" (Error (`No_such_key key)) res;
  let%bind _ = Orewa.lpush conn key ~element in
  let%bind res = Orewa.lset conn ~key 0 ~element in
  Alcotest.(check ue) "Setting existent index of list" (Ok ()) res;
  let%bind res = Orewa.lset conn ~key 1 ~element in
  Alcotest.(check ue)
    "Setting non-existent index of list"
    (Error (`Index_out_of_range key))
    res;
  return ()

let test_ltrim () =
  Orewa.with_connection ~host @@ fun conn ->
  let key = random_key () in
  let element = random_key () in
  let elements = 10 in
  let%bind _ =
    List.init elements ~f:(fun _ -> Orewa.lpush conn key ~element) |> Deferred.all
  in
  let%bind res = Orewa.ltrim conn ~start:0 ~end':4 key in
  Alcotest.(check ue) "Trimming list" (Ok ()) res;
  let%bind res = Orewa.llen conn key in
  Alcotest.(check ie) "List is trimmed" (Ok 5) res;
  return ()

let test_hset () =
  Orewa.with_connection ~host @@ fun conn ->
  let key = random_key () in
  let random_element () =
    let field = random_key () in
    let value = random_key () in
    field, value
  in
  let element = random_element () in
  let%bind res = Orewa.hset conn ~element key in
  Alcotest.(check ie) "Set single element" (Ok 1) res;
  let%bind res = Orewa.hset conn ~element key in
  Alcotest.(check ie) "Resetting is no-op" (Ok 0) res;
  let%bind res =
    Orewa.hset
      conn
      ~element:(random_element ())
      ~elements:[random_element (); random_element ()]
      key
  in
  Alcotest.(check ie) "Set multiple elements" (Ok 3) res;
  return ()

let test_hget () =
  Orewa.with_connection ~host @@ fun conn ->
  let key = random_key () in
  let field = random_key () in
  let value = random_key () in
  let element = field, value in
  let%bind _ = Orewa.hset conn ~element key in
  let%bind res = Orewa.hget conn ~field key in
  Alcotest.(check se) "Getting the value that was set" (Ok value) res;
  return ()

let test_hmget () =
  Orewa.with_connection ~host @@ fun conn ->
  let key = random_key () in
  let field = random_key () in
  let value = random_key () in
  let element = field, value in
  let%bind res = Orewa.hmget conn ~fields:[field] key in
  let expected = String.Map.of_alist_exn [] in
  Alcotest.(check sme) "Getting empty key" (Ok expected) res;
  let%bind _ = Orewa.hset conn ~element key in
  let%bind res = Orewa.hmget conn ~fields:[field] key in
  let expected = String.Map.of_alist_exn [element] in
  Alcotest.(check sme) "Getting the value that was set" (Ok expected) res;
  return ()

let test_hgetall () =
  Orewa.with_connection ~host @@ fun conn ->
  let key = random_key () in
  let%bind res = Orewa.hgetall conn key in
  let expected = String.Map.of_alist_exn [] in
  Alcotest.(check sme) "Getting an empty map on empty key" (Ok expected) res;
  let field = random_key () in
  let value = random_key () in
  let element = field, value in
  let%bind _ = Orewa.hset conn ~element key in
  let%bind res = Orewa.hgetall conn key in
  let expected = String.Map.of_alist_exn [element] in
  Alcotest.(check sme) "Getting a map of elements" (Ok expected) res;
  return ()

let test_hdel () =
  Orewa.with_connection ~host @@ fun conn ->
  let key = random_key () in
  let field = random_key () in
  let value = random_key () in
  let element = field, value in
  let%bind res = Orewa.hdel conn ~field key in
  Alcotest.(check ie) "Deleting from empty hashtable" (Ok 0) res;
  let field' = random_key () in
  let element' = field', value in
  let field'' = random_key () in
  let element'' = field'', value in
  let%bind _ = Orewa.hset conn ~element ~elements:[element'; element''] key in
  let%bind res = Orewa.hdel conn ~field key in
  Alcotest.(check ie) "Single delete from filled hashtable" (Ok 1) res;
  let%bind res = Orewa.hdel conn ~field:field' ~fields:[field''] key in
  Alcotest.(check ie) "Single delete from filled hashtable" (Ok 2) res;
  return ()

let test_hexists () =
  Orewa.with_connection ~host @@ fun conn ->
  let key = random_key () in
  let field = random_key () in
  let value = random_key () in
  let element = field, value in
  let%bind res = Orewa.hexists conn ~field key in
  Alcotest.(check be) "Asking for nonexisting field on missing key" (Ok false) res;
  let%bind _ = Orewa.hset conn ~element key in
  let%bind res = Orewa.hexists conn ~field key in
  Alcotest.(check be) "Asking for existing key" (Ok true) res;
  let%bind _ = Orewa.hdel conn ~field key in
  let%bind res = Orewa.hexists conn ~field key in
  Alcotest.(check be) "Asking for deleted key" (Ok false) res;
  return ()

let test_hincrby () =
  Orewa.with_connection ~host @@ fun conn ->
  let key = random_key () in
  let field = random_key () in
  let value = 42 in
  let%bind res = Orewa.hincrby conn ~field key value in
  Alcotest.(check ie) "Incrementing missing key" (Ok value) res;
  let%bind res = Orewa.hincrby conn ~field key value in
  Alcotest.(check ie) "Incrementing existing key" (Ok (2 * value)) res;
  return ()

let tests =
  Alcotest_async.
    [ test_case "ECHO" `Slow test_echo;
      test_case "SET" `Slow test_set;
      test_case "GET" `Slow test_get;
      test_case "MGET" `Slow test_mget;
      test_case "MSET" `Slow test_mset;
      test_case "MSETNX" `Slow test_msetnx;
      test_case "GETRANGE" `Slow test_getrange;
      test_case "Large SET/GET" `Slow test_large_set_get;
      test_case "RPOPLPUSH" `Slow test_rpoplpush;
      test_case "SET with expiry" `Slow test_set_expiry;
      test_case "LPUSH" `Slow test_lpush;
      test_case "RPUSH" `Slow test_rpush;
      test_case "LPOP" `Slow test_lpop;
      test_case "RPOP" `Slow test_rpop;
      test_case "LRANGE" `Slow test_lpush_lrange;
      test_case "LREM" `Slow test_lrem;
      test_case "LSET" `Slow test_lset;
      test_case "LTRIM" `Slow test_ltrim;
      test_case "Large LRANGE" `Slow test_large_lrange;
      test_case "APPEND" `Slow test_append;
      test_case "AUTH" `Slow test_auth;
      test_case "BGREWRITEAOF" `Slow test_bgrewriteaof;
      test_case "BGSAVE" `Slow test_bgsave;
      test_case "BITCOUNT" `Slow test_bitcount;
      test_case "BITFIELD" `Slow test_bitfield;
      test_case "BITOP" `Slow test_bitop;
      test_case "BITPOS" `Slow test_bitpos;
      test_case "GETBIT" `Slow test_getbit;
      test_case "GETSET" `Slow test_getset;
      test_case "STRLEN" `Slow test_strlen;
      test_case "SETBIT" `Slow test_setbit;
      test_case "DECR" `Slow test_decr;
      test_case "DECRBY" `Slow test_decrby;
      test_case "INCR" `Slow test_incr;
      test_case "INCRBY" `Slow test_incrby;
      test_case "INCRBYFLOAT" `Slow test_incrbyfloat;
      test_case "SELECT" `Slow test_select;
      test_case "DEL" `Slow test_del;
      test_case "EXISTS" `Slow test_exists;
      test_case "EXPIRE" `Slow test_expire;
      test_case "EXPIREAT" `Slow test_expireat;
      test_case "KEYS" `Slow test_keys;
      test_case "SADD" `Slow test_sadd;
      test_case "SCARD" `Slow test_scard;
      test_case "SDIFF" `Slow test_sdiff;
      test_case "SDIFFSTORE" `Slow test_sdiffstore;
      test_case "SINTER" `Slow test_sinter;
      test_case "SINTERSTORE" `Slow test_sinterstore;
      test_case "SISMEMBER" `Slow test_sismember;
      test_case "SMEMBERS" `Slow test_smembers;
      test_case "SPOP" `Slow test_spop;
      test_case "SRANDMEMBER" `Slow test_srandmember;
      test_case "SREM" `Slow test_srem;
      test_case "SUNION" `Slow test_sunion;
      test_case "SUNIONSTORE" `Slow test_sunionstore;
      test_case "SSCAN" `Slow test_sscan;
      test_case "SCAN" `Slow test_scan;
      test_case "MOVE" `Slow test_move;
      test_case "PERSIST" `Slow test_persist;
      test_case "RANDOMKEY" `Slow test_randomkey;
      test_case "RENAME" `Slow test_rename;
      test_case "RENAMENX" `Slow test_renamenx;
      test_case "SORT" `Slow test_sort;
      test_case "TTL" `Slow test_ttl;
      test_case "TYPE" `Slow test_type';
      test_case "DUMP" `Slow test_dump;
      test_case "RESTORE" `Slow test_restore;
      test_case "PIPELINE" `Slow test_pipelining;
      test_case "CLOSE" `Slow test_close;
      test_case "LINSERT" `Slow test_linsert;
      test_case "LLEN" `Slow test_llen;
      test_case "LINDEX" `Slow test_lindex;
      test_case "HSET" `Slow test_hset;
      test_case "HGET" `Slow test_hget;
      test_case "HMGET" `Slow test_hmget;
      test_case "HGETALL" `Slow test_hgetall;
      test_case "HDEL" `Slow test_hdel;
      test_case "HEXISTS" `Slow test_hexists;
      test_case "HINCRBY" `Slow test_hincrby ]

let () =
  Log.Global.set_level `Debug;
  Alcotest.run Caml.__MODULE__ ["integration", tests]
