open Core
open Async

type t = {
  reader : Reader.t;
  writer : Writer.t;
}

let construct_request commands =
  commands
  |> List.map ~f:(fun cmd -> Resp.Bulk cmd)
  |> (fun xs -> Resp.Array xs)
  |> Resp.encode

let submit_request writer command =
  construct_request command
  |> Writer.write writer

let request { reader; writer } req =
  submit_request writer req;
  Parser.read_resp reader

let echo t message =
  let open Deferred.Result.Let_syntax in
  match%bind request t ["ECHO"; message] with
  | Resp.Bulk v -> return v
  | _ -> Deferred.return @@ Error `Unexpected

let set t ~key value =
  let open Deferred.Result.Let_syntax in
  match%bind request t ["SET"; key; value] with
  | Resp.String "OK" -> return ()
  | _ -> Deferred.return @@ Error `Unexpected

let get t key =
  let open Deferred.Result.Let_syntax in
  match%bind request t ["GET"; key] with
  | Resp.Bulk v -> return v
  | _ -> Deferred.return @@ Error `Unexpected

let lpush t ~key value =
  let open Deferred.Result.Let_syntax in
  match%bind request t ["LPUSH"; key; value] with
  | Resp.Integer n -> return n
  | _ -> Deferred.return @@ Error `Unexpected

let lrange t ~key ~start ~stop =
  let open Deferred.Result.Let_syntax in
  match%bind request t ["LRANGE"; key; string_of_int start; string_of_int stop] with
  | Resp.Array xs ->
    List.map xs ~f:(function
      | Resp.Bulk v -> Ok v
      | _ -> Error `Unexpected)
    |> Result.all
    |> Deferred.return
  | _ -> Deferred.return @@ Error `Unexpected

let init reader writer =
  { reader; writer }

let connect ?(port=6379) ~host f =
  let where = Tcp.Where_to_connect.of_host_and_port @@ Host_and_port.create ~host ~port  in
  Tcp.with_connection where @@ fun _socket reader writer ->
    let t = init reader writer in
    f t
