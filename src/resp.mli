open! Core

type redis_error = string [@@deriving show, eq]

type t =
  | String of string
  | Error of redis_error
  | Integer of int
  | Bulk of string
  | Array of t list
  | Null
[@@deriving show, eq]

val terminator : bool -> string

val encode : t -> string
