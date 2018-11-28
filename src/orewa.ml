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

type bitop =
  | AND
  | OR
  | XOR
  | NOT

let string_of_bitop = function
  | AND -> "AND"
  | OR -> "OR"
  | XOR -> "XOR"
  | NOT -> "NOT"

let bitop t ~destkey ?(keys=[]) ~key op =
  let open Deferred.Result.Let_syntax in
  match%bind request t (["BITOP"; (string_of_bitop op); destkey; key] @ keys) with
  | Resp.Integer n -> return n
  | _ -> Deferred.return @@ Error `Unexpected

let decr t key =
  let open Deferred.Result.Let_syntax in
  match%bind request t ["DECR"; key] with
  | Resp.Integer n -> return n
  | _ -> Deferred.return @@ Error `Unexpected

let decrby t key decrement =
  let open Deferred.Result.Let_syntax in
  match%bind request t ["DECRBY"; key; (string_of_int decrement)] with
  | Resp.Integer n -> return n
  | _ -> Deferred.return @@ Error `Unexpected

let incr t key =
  let open Deferred.Result.Let_syntax in
  match%bind request t ["INCR"; key] with
  | Resp.Integer n -> return n
  | _ -> Deferred.return @@ Error `Unexpected

let incrby t key increment =
  let open Deferred.Result.Let_syntax in
  match%bind request t ["INCRBY"; key; (string_of_int increment)] with
  | Resp.Integer n -> return n
  | _ -> Deferred.return @@ Error `Unexpected

let select t index =
  let open Deferred.Result.Let_syntax in
  match%bind request t ["SELECT"; (string_of_int index)] with
  | Resp.String "OK" -> return ()
  | _ -> Deferred.return @@ Error `Unexpected

let del t ?(keys=[]) key =
  let open Deferred.Result.Let_syntax in
  match%bind request t (["DEL"; key] @ keys) with
  | Resp.Integer n -> return n
  | _ -> Deferred.return @@ Error `Unexpected

let exists t ?(keys=[]) key =
  let open Deferred.Result.Let_syntax in
  match%bind request t (["EXISTS"; key] @ keys) with
  | Resp.Integer n -> return n
  | _ -> Deferred.return @@ Error `Unexpected

let expire t key span =
  let open Deferred.Result.Let_syntax in
  let milliseconds = Time.Span.to_ms span in
  (* rounded to nearest millisecond *)
  let expire = Printf.sprintf "%.0f" milliseconds in
  match%bind request t ["PEXPIRE"; key; expire] with
  | Resp.Integer n -> return n
  | _ -> Deferred.return @@ Error `Unexpected

let expireat t key dt =
  let open Deferred.Result.Let_syntax in
  let since_epoch = dt |> Time.to_span_since_epoch |> Time.Span.to_ms in
  let expire = Printf.sprintf "%.0f" since_epoch in
  match%bind request t ["PEXPIREAT"; key; expire] with
  | Resp.Integer n -> return n
  | _ -> Deferred.return @@ Error `Unexpected

let keys t pattern =
  let open Deferred.Result.Let_syntax in
  match%bind request t ["KEYS"; pattern] with
  | Resp.Array xs ->
    List.map xs ~f:(function
      | Resp.Bulk key -> Ok key
      | _ -> Error `Unexpected)
    |> Result.all
    |> Deferred.return
  | _ -> Deferred.return @@ Error `Unexpected

let scan ?pattern ?count t =
  let pattern = match pattern with
    | Some pattern -> ["MATCH"; pattern]
    | None -> []
  in
  let count = match count with
    | Some count -> ["COUNT"; string_of_int count]
    | None -> []
  in
  Pipe.create_reader ~close_on_exception:false @@ fun writer ->
    Deferred.repeat_until_finished "0" @@ fun cursor ->
      match%bind request t (["SCAN"; cursor] @ pattern @ count) with
      | Ok Resp.Array [Resp.Bulk cursor; Resp.Array from] ->
        let from = from
          |> List.map ~f:(function
            | Resp.Bulk s -> s
            | _ -> failwith "unexpected")
          |> Queue.of_list
        in
        let%bind () = Pipe.transfer_in writer ~from in
        (match cursor with
          | "0" -> return @@ `Finished ()
          | cursor -> return @@ `Repeat cursor)
      | _ -> failwith "unexpected"

let move t key db =
  let open Deferred.Result.Let_syntax in
  match%bind request t ["MOVE"; key; (string_of_int db)] with
  | Resp.Integer 0 -> return false
  | Resp.Integer 1 -> return true
  | _ -> Deferred.return @@ Error `Unexpected

let persist t key =
  let open Deferred.Result.Let_syntax in
  match%bind request t ["PERSIST"; key] with
  | Resp.Integer 0 -> return false
  | Resp.Integer 1 -> return true
  | _ -> Deferred.return @@ Error `Unexpected

let randomkey t =
  let open Deferred.Result.Let_syntax in
  match%bind request t ["RANDOMKEY"] with
  | Resp.Bulk s -> return s
  | _ -> Deferred.return @@ Error `Unexpected

let init reader writer =
  { reader; writer }

let connect ?(port=6379) ~host f =
  let where = Tcp.Where_to_connect.of_host_and_port @@ Host_and_port.create ~host ~port  in
  Tcp.with_connection where @@ fun _socket reader writer ->
    let t = init reader writer in
    f t
