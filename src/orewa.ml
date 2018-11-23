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

type exist = Not_if_exists | Only_if_exists

let set t ~key ?expire ?exist value =
  let open Deferred.Result.Let_syntax in
  let expiry = match expire with
    | None -> []
    | Some span -> ["PX"; span |> Time.Span.to_ms |> int_of_float |> string_of_int]
  in
  let existence = match exist with
    | None -> []
    | Some Not_if_exists -> ["NX"]
    | Some Only_if_exists -> ["PX"]
  in
  let command = ["SET"; key; value;] @ expiry @ existence in
  match%bind request t command with
  | Resp.String "OK" -> return ()
  | _ -> Deferred.return @@ Error `Unexpected

let get t key =
  let open Deferred.Result.Let_syntax in
  match%bind request t ["GET"; key] with
  | Resp.Bulk v -> return @@ Some v
  | Resp.Null -> return @@ None
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

let append t ~key value =
  let open Deferred.Result.Let_syntax in
  match%bind request t ["APPEND"; key; value] with
  | Resp.Integer n -> return n
  | _ -> Deferred.return @@ Error `Unexpected

let auth t password =
  let open Deferred.Result.Let_syntax in
  match%bind request t ["AUTH"; password] with
  | Resp.String "OK" -> return ()
  | Resp.Error e -> Deferred.return @@ Error (`Redis_error e)
  | _ -> Deferred.return @@ Error `Unexpected

let bgrewriteaof t =
  let open Deferred.Result.Let_syntax in
  match%bind request t ["BGREWRITEAOF"] with
  (* the documentation says it returns OK, but that's not true *)
  | Resp.String v -> return v
  | _ -> Deferred.return @@ Error `Unexpected

let bgsave t =
  let open Deferred.Result.Let_syntax in
  match%bind request t ["BGSAVE"] with
  | Resp.String v -> return v
  | _ -> Deferred.return @@ Error `Unexpected

let bitcount t ?range key =
  let open Deferred.Result.Let_syntax in
  let range = match range with
    | None -> []
    | Some (start, end_) -> [(string_of_int start); (string_of_int end_)]
  in
  match%bind request t (["BITCOUNT"; key] @ range) with
  | Resp.Integer n -> return n
  | _ -> Deferred.return @@ Error `Unexpected

let init reader writer =
  { reader; writer }

let connect ?(port=6379) ~host f =
  let where = Tcp.Where_to_connect.of_host_and_port @@ Host_and_port.create ~host ~port  in
  Tcp.with_connection where @@ fun _socket reader writer ->
    let t = init reader writer in
    f t
