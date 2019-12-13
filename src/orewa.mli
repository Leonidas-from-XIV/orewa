open Core
open Async

type t

type common_error =
  [ `Connection_closed
  | `Unexpected ]
[@@deriving show, eq]

type wrong_type = [`Wrong_type of string] [@@deriving show, eq]

val echo : t -> string -> (string, [> common_error]) Deferred.Result.t

val set
  :  t ->
  key:string ->
  ?expire:Time.Span.t ->
  ?exist:[`Always | `Not_if_exists | `Only_if_exists] ->
  string ->
  (bool, [> common_error]) Deferred.Result.t

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

val lpush
  :  t ->
  ?exist:[`Always | `Only_if_exists] ->
  element:string ->
  ?elements:string list ->
  string ->
  (int, [> common_error | wrong_type]) Deferred.Result.t

val rpush
  :  t ->
  ?exist:[`Always | `Only_if_exists] ->
  element:string ->
  ?elements:string list ->
  string ->
  (int, [> common_error | wrong_type]) Deferred.Result.t

val lpop : t -> string -> (string option, [> common_error | wrong_type]) Deferred.Result.t

val rpop : t -> string -> (string option, [> common_error | wrong_type]) Deferred.Result.t

val lrange
  :  t ->
  key:string ->
  start:int ->
  stop:int ->
  (string list, [> common_error]) Deferred.Result.t

val rpoplpush
  :  t ->
  source:string ->
  destination:string ->
  (string, [> common_error | wrong_type]) Deferred.Result.t

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

type intsize =
  | Signed of int
  | Unsigned of int

type offset =
  | Absolute of int
  | Relative of int

type fieldop =
  | Get of intsize * offset
  | Set of intsize * offset * int
  | Incrby of intsize * offset * int

val bitfield
  :  t ->
  ?overflow:overflow ->
  string ->
  fieldop list ->
  (int option list, [> common_error]) Deferred.Result.t

type bitop =
  | AND
  | OR
  | XOR
  | NOT

val bitop
  :  t ->
  destkey:string ->
  ?keys:string list ->
  key:string ->
  bitop ->
  (int, [> common_error]) Deferred.Result.t

type bit =
  | Zero
  | One
[@@deriving show, eq]

val bitpos
  :  t ->
  ?start:int ->
  ?end':int ->
  string ->
  bit ->
  (int option, [> common_error]) Deferred.Result.t

val getbit : t -> string -> int -> (bit, [> common_error]) Deferred.Result.t

val setbit : t -> string -> int -> bit -> (bit, [> common_error]) Deferred.Result.t

val decr : t -> string -> (int, [> common_error]) Deferred.Result.t

val decrby : t -> string -> int -> (int, [> common_error]) Deferred.Result.t

val incr : t -> string -> (int, [> common_error]) Deferred.Result.t

val incrby : t -> string -> int -> (int, [> common_error]) Deferred.Result.t

val incrbyfloat : t -> string -> float -> (float, [> common_error]) Deferred.Result.t

val select : t -> int -> (unit, [> common_error]) Deferred.Result.t

val del : t -> ?keys:string list -> string -> (int, [> common_error]) Deferred.Result.t

val exists : t -> ?keys:string list -> string -> (int, [> common_error]) Deferred.Result.t

val expire : t -> string -> Time.Span.t -> (int, [> common_error]) Deferred.Result.t

val expireat : t -> string -> Time.t -> (int, [> common_error]) Deferred.Result.t

val keys : t -> string -> (string list, [> common_error]) Deferred.Result.t

val sadd
  :  t ->
  key:string ->
  ?members:string list ->
  string ->
  (int, [> common_error]) Deferred.Result.t

val scan : ?pattern:string -> ?count:int -> t -> string Pipe.Reader.t

val scard : t -> string -> (int, [> common_error]) Deferred.Result.t

val sdiff
  :  t ->
  ?keys:string list ->
  string ->
  (string list, [> common_error]) Deferred.Result.t

val sdiffstore
  :  t ->
  destination:string ->
  ?keys:string list ->
  key:string ->
  (int, [> common_error]) Deferred.Result.t

val sinter
  :  t ->
  ?keys:string list ->
  string ->
  (string list, [> common_error]) Deferred.Result.t

val sinterstore
  :  t ->
  destination:string ->
  ?keys:string list ->
  key:string ->
  (int, [> common_error]) Deferred.Result.t

val sismember : t -> key:string -> string -> (bool, [> common_error]) Deferred.Result.t

val smembers : t -> string -> (string list, [> common_error]) Deferred.Result.t

val smove
  :  t ->
  source:string ->
  destination:string ->
  string ->
  (bool, [> common_error]) Deferred.Result.t

val spop : t -> ?count:int -> string -> (string list, [> common_error]) Deferred.Result.t

val srandmember
  :  t ->
  ?count:int ->
  string ->
  (string list, [> common_error]) Deferred.Result.t

val srem
  :  t ->
  key:string ->
  ?members:string list ->
  string ->
  (int, [> common_error]) Deferred.Result.t

val sunion
  :  t ->
  ?keys:string list ->
  string ->
  (string list, [> common_error]) Deferred.Result.t

val sunionstore
  :  t ->
  destination:string ->
  ?keys:string list ->
  key:string ->
  (int, [> common_error]) Deferred.Result.t

val sscan : t -> ?pattern:string -> ?count:int -> string -> string Pipe.Reader.t

val move : t -> string -> int -> (bool, [> common_error]) Deferred.Result.t

val persist : t -> string -> (bool, [> common_error]) Deferred.Result.t

val randomkey : t -> (string, [> common_error]) Deferred.Result.t

val rename : t -> string -> string -> (unit, [> common_error]) Deferred.Result.t

val renamenx : t -> key:string -> string -> (bool, [> common_error]) Deferred.Result.t

type order =
  | Asc
  | Desc

val sort
  :  t ->
  ?by:string ->
  ?limit:int * int ->
  ?get:string list ->
  ?order:order ->
  ?alpha:bool ->
  ?store:string ->
  string ->
  ([> `Count of int | `Sorted of string list], [> common_error]) Deferred.Result.t

val ttl
  :  t ->
  string ->
  ( Time.Span.t,
    [> `No_such_key of string | `Not_expiring of string | common_error] )
  Deferred.Result.t

val type' : t -> string -> (string option, [> common_error]) Deferred.Result.t

val dump : t -> string -> (string option, [> common_error]) Deferred.Result.t

val restore
  :  t ->
  key:string ->
  ?ttl:Time.Span.t ->
  ?replace:bool ->
  string ->
  (unit, [> common_error]) Deferred.Result.t

val lindex : t -> string -> int -> (string option, [> common_error]) Deferred.Result.t

type position =
  | Before
  | After

val linsert
  :  t ->
  key:string ->
  position ->
  element:string ->
  pivot:string ->
  (int, [> common_error]) Deferred.Result.t

val llen : t -> string -> (int, [> common_error]) Deferred.Result.t

val lrem
  :  t ->
  key:string ->
  int ->
  element:string ->
  (int, [> common_error]) Deferred.Result.t

val lset
  :  t ->
  key:string ->
  int ->
  element:string ->
  ( unit,
    [> common_error | `No_such_key of string | `Index_out_of_range of string] )
  Deferred.Result.t

val ltrim
  :  t ->
  start:int ->
  end':int ->
  string ->
  (unit, [> common_error]) Deferred.Result.t

val hset
  :  t ->
  element:string * string ->
  ?elements:(string * string) list ->
  string ->
  (int, [> common_error]) Deferred.Result.t

val hget : t -> field:string -> string -> (string, [> common_error]) Deferred.Result.t

val hmget
  :  t ->
  fields:string list ->
  string ->
  (string String.Map.t, [> common_error]) Deferred.Result.t

val hgetall : t -> string -> (string String.Map.t, [> common_error]) Deferred.Result.t

val hdel
  :  t ->
  ?fields:string list ->
  field:string ->
  string ->
  (int, [> common_error]) Deferred.Result.t

val hexists : t -> field:string -> string -> (bool, [> common_error]) Deferred.Result.t

val hincrby
  :  t ->
  field:string ->
  string ->
  int ->
  (int, [> common_error]) Deferred.Result.t

val hincrbyfloat
  :  t ->
  field:string ->
  string ->
  float ->
  (float, [> common_error]) Deferred.Result.t

val hkeys : t -> string -> (string list, [> common_error]) Deferred.Result.t

val hlen : t -> string -> (int, [> common_error]) Deferred.Result.t

val connect : ?port:int -> host:string -> t Deferred.t

val close : t -> unit Deferred.t

val with_connection : ?port:int -> host:string -> (t -> 'a Deferred.t) -> 'a Deferred.t
