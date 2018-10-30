open Core
open Async

module Resp = Resp

type t = {
  reader : Reader.t;
  writer : Writer.t;
}

let record_length iobuf =
  let cr = '\r' in
  let lf = '\n' in
  match Iobuf.Peek.index iobuf cr, Iobuf.Peek.index iobuf lf with
  | Some cr_index, Some lf_index when lf_index = Int.succ cr_index -> Some (Int.succ lf_index)
  | _ -> None

let consume_record ~len iobuf =
  Iobuf.Consume.stringo ~len iobuf
  |> String.subo ~len:(len - 2)

let rec handle_chunk iobuf =
  let type' = Iobuf.Consume.char iobuf in
  match type' with
  | '+' ->
    (* Simple string *)
    (match record_length iobuf with
    | Some len ->
      let content = consume_record ~len iobuf in
      Log.Global.error "READ: %s" (String.escaped content);
      return @@ `Stop (Resp.String content)
    | None ->
      return @@ `Continue)
  | '-' ->
    (* Error, which is also a simple string *)
    (match record_length iobuf with
    | Some len ->
      let content = consume_record ~len iobuf in
      Log.Global.error "READ: %s" (String.escaped content);
      return @@ `Stop (Resp.Error content)
    | None ->
      return @@ `Continue)
  | '$' ->
    (* Bulk string *)
    (match record_length iobuf with
    | Some len ->
      let len = consume_record ~len iobuf |> int_of_string in
      Log.Global.error "LEN %d" len;
      (* read trailing \r\n and discard *)
      let content = consume_record ~len:(len + 2) iobuf in
      Log.Global.error "CONTENT: '%s'" (String.escaped content);
      return @@ `Stop (Resp.Bulk content)
    | None ->
      return `Continue)
  | ':' ->
    (* Integer *)
    (match record_length iobuf with
    | Some len ->
      let value = consume_record ~len iobuf |> int_of_string in
      return @@ `Stop (Resp.Integer value)
    | None ->
      return `Continue)
  | '*' ->
    (* Array *)
    (match record_length iobuf with
    | Some len ->
      (let elements = consume_record ~len iobuf |> int_of_string in
      Log.Global.error "ELEMENTS TO READ: %d" elements;
      let rec loop xs = function
        | 0 -> return @@ `Stop xs
        | remaining ->
          match%bind handle_chunk iobuf with
          | `Stop parsed -> loop (parsed::xs) (Int.pred remaining)
          | `Continue -> return `Continue
      in
      match%bind loop [] elements with
      | `Continue -> return `Continue
      | `Stop xs -> return @@ `Stop (Resp.Array xs))
    | None ->
      return `Continue)
  | unknown ->
    (* Unknown match *)
    Log.Global.error "Unparseable type tag %C" unknown;
    return @@ `Stop Resp.Null

let read_resp reader =
  let%bind res = Reader.read_one_iobuf_at_a_time reader ~handle_chunk in
  match res with
  | `Eof -> return @@ Error `Eof
  | `Stopped v -> return @@ Ok v
  | `Eof_with_unconsumed_data _data -> return @@ Error `Connection_closed

let construct_request commands =
  commands |> List.map ~f:(fun cmd -> Resp.Bulk cmd) |> (fun xs -> Resp.Array xs) |> Resp.encode

let submit_request writer command =
  construct_request command
  |> Writer.write writer

let echo { reader; writer } message =
  submit_request writer ["ECHO"; message];
  read_resp reader

let set { reader; writer } ~key value =
  let open Deferred.Result.Let_syntax in
  submit_request writer ["SET"; key; value];
  match%bind read_resp reader with
  | Resp.String "OK" -> return ()
  | _ -> Deferred.return @@ Error `Unexpected

let get { reader; writer } key =
  submit_request writer ["GET"; key];
  read_resp reader

let lpush { reader; writer } ~key value =
  let open Deferred.Result.Let_syntax in
  submit_request writer ["LPUSH"; key; value];
  match%bind read_resp reader with
  | Resp.Integer n -> return n
  | _ -> Deferred.return @@ Error `Unexpected

let lrange { reader; writer } ~key ~start ~stop =
  submit_request writer ["LRANGE"; key; string_of_int start; string_of_int stop];
  read_resp reader

let init reader writer =
  { reader; writer }

let connect ?(port=6379) ~host f =
  let where = Tcp.Where_to_connect.of_host_and_port @@ Host_and_port.create ~host ~port  in
  Tcp.with_connection where @@ fun _socket reader writer ->
    let t = init reader writer in
    f t
