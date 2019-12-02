open! Core
open Async

val read_resp
  :  Reader.t ->
  (Resp.t, [> `Connection_closed | `Unexpected]) Deferred.Result.t
