open Core
open Async

type t

type common_error =
  [ `Connection_closed
  | `Eof
  | `Unexpected ]

val echo : t -> string -> (string, [> common_error]) Deferred.Result.t

type exist =
  | Not_if_exists
  | Only_if_exists

val set
  :  t ->
  key:string ->
  ?expire:Time.Span.t ->
  ?exist:exist ->
  string ->
  (unit, [> common_error]) Deferred.Result.t

val get : t -> string -> (string option, [> common_error]) Deferred.Result.t

val getrange
  :  t ->
  start:int ->
  end':int ->
  string ->
  (string, [> common_error]) Deferred.Result.t

val getset
  :  t ->
  key:string ->
  string ->
  (string option, [> common_error]) Deferred.Result.t

val strlen : t -> string -> (int, [> common_error]) Deferred.Result.t

val mget : t -> string list -> (string option list, [> common_error]) Deferred.Result.t

val mset : t -> (string * string) list -> (unit, [> common_error]) Deferred.Result.t

val msetnx : t -> (string * string) list -> (bool, [> common_error]) Deferred.Result.t

val lpush : t -> key:string -> string -> (int, [> common_error]) Deferred.Result.t

val lrange
  :  t ->
  key:string ->
  start:int ->
  stop:int ->
  (string list, [> common_error]) Deferred.Result.t

val append : t -> key:string -> string -> (int, [> common_error]) Deferred.Result.t

val auth
  :  t ->
  string ->
  (unit, [> `Redis_error of string | common_error]) Deferred.Result.t

val bgrewriteaof : t -> (string, [> common_error]) Deferred.Result.t

val bgsave : t -> (string, [> common_error]) Deferred.Result.t

val bitcount
  :  t ->
  ?range:int * int ->
  string ->
  (int, [> common_error]) Deferred.Result.t

type overflow =
  | Wrap
  | Sat
  | Fail

val string_of_overflow : overflow -> string

type intsize = string

type offset =
  | Absolute of int
  | Relative of int

val string_of_offset : offset -> intsize

type fieldop =
  | Get of intsize * offset
  | Set of intsize * offset * int
  | Incrby of intsize * offset * int

val bitfield
  :  t ->
  ?overflow:overflow ->
  intsize ->
  fieldop list ->
  (int option list, [> common_error]) Deferred.Result.t

type bitop =
  | AND
  | OR
  | XOR
  | NOT

val string_of_bitop : bitop -> intsize

val bitop
  :  t ->
  destkey:intsize ->
  ?keys:intsize list ->
  key:intsize ->
  bitop ->
  (int, [> common_error]) Deferred.Result.t

type bit =
  | Zero
  | One

val pp_bit : Format.formatter -> bit -> unit

val show_bit : bit -> intsize

val equal_bit : bit -> bit -> bool

val string_of_bit : bit -> intsize

val bitpos
  :  t ->
  ?start:int ->
  ?end':int ->
  intsize ->
  bit ->
  (int option, [> common_error]) Deferred.Result.t

val getbit : t -> intsize -> int -> (bit, [> common_error]) Deferred.Result.t

val setbit : t -> intsize -> int -> bit -> (bit, [> common_error]) Deferred.Result.t

val decr : t -> intsize -> (int, [> common_error]) Deferred.Result.t

val decrby : t -> intsize -> int -> (int, [> common_error]) Deferred.Result.t

val incr : t -> intsize -> (int, [> common_error]) Deferred.Result.t

val incrby : t -> intsize -> int -> (int, [> common_error]) Deferred.Result.t

val incrbyfloat : t -> intsize -> float -> (float, [> common_error]) Deferred.Result.t

val select : t -> int -> (unit, [> common_error]) Deferred.Result.t

val del : t -> ?keys:intsize list -> intsize -> (int, [> common_error]) Deferred.Result.t

val exists
  :  t ->
  ?keys:intsize list ->
  intsize ->
  (int, [> common_error]) Deferred.Result.t

val expire : t -> intsize -> Time.Span.t -> (int, [> common_error]) Deferred.Result.t

val expireat : t -> intsize -> Time.t -> (int, [> common_error]) Deferred.Result.t

val keys : t -> intsize -> (intsize list, [> common_error]) Deferred.Result.t

val scan : ?pattern:intsize -> ?count:int -> t -> intsize Pipe.Reader.t

val move : t -> intsize -> int -> (bool, [> common_error]) Deferred.Result.t

val persist : t -> intsize -> (bool, [> common_error]) Deferred.Result.t

val randomkey : t -> (intsize, [> common_error]) Deferred.Result.t

val rename : t -> intsize -> intsize -> (unit, [> common_error]) Deferred.Result.t

val renamenx : t -> key:intsize -> intsize -> (bool, [> common_error]) Deferred.Result.t

type order =
  | Asc
  | Desc

val sort
  :  t ->
  ?by:intsize ->
  ?limit:int * int ->
  ?get:intsize list ->
  ?order:order ->
  ?alpha:bool ->
  ?store:intsize ->
  intsize ->
  ([> `Count of int | `Sorted of intsize list], [> common_error]) Deferred.Result.t

val ttl
  :  t ->
  intsize ->
  ( Time.Span.t,
    [> `No_such_key of intsize | `Not_expiring of intsize | common_error] )
  Deferred.Result.t

val type' : t -> intsize -> (intsize option, [> common_error]) Deferred.Result.t

val dump : t -> intsize -> (intsize option, [> common_error]) Deferred.Result.t

val restore
  :  t ->
  key:intsize ->
  ?ttl:Time.Span.t ->
  ?replace:bool ->
  intsize ->
  (unit, [> common_error]) Deferred.Result.t

val connect : ?port:int -> host:intsize -> t Deferred.t

val close : t -> unit Deferred.t

val with_connection : ?port:int -> host:intsize -> (t -> 'a Deferred.t) -> 'a Deferred.t
