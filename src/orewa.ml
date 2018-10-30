open Core
open Async

type t = {
  reader : Reader.t;
  writer : Writer.t;
}

let crlf_ends_at iobuf =
  let cr = '\r' in
  let lf = '\n' in
  match Iobuf.Peek.index iobuf cr, Iobuf.Peek.index iobuf lf with
  | Some cr_index, Some lf_index when lf_index = Int.succ cr_index -> Some lf_index
  | _ -> None

let handle_chunk iobuf =
  let type' = Iobuf.Peek.char iobuf ~pos:0 in
  match type' with
  | '+' ->
    (match crlf_ends_at iobuf with
    | Some end_index -> 
      let value = Iobuf.Consume.string ~str_pos:0 ~len:(Int.succ end_index) iobuf in
      let parsed = Resp.decode value in
      Log.Global.debug "READ: %s" value;
      return @@ `Stop parsed
    | None ->
      return @@ `Continue)
  | _ -> return @@ `Stop Resp.Null

let read_resp reader =
  let%bind res = Reader.read_one_iobuf_at_a_time reader ~handle_chunk in
  match res with
  | `Eof -> return @@ Error `Eof
  | `Stopped v -> return @@ Ok v
  | `Eof_with_unconsumed_data _data -> return @@ Error `Connection_closed

let get { reader; writer } key =
  let request = Resp.Array [Resp.Bulk "GET"; Resp.Bulk key] in
  let request = Resp.encode request in
  Writer.write writer request;
  match%bind Reader.read_line reader with
  | `Ok s -> return @@ Ok s
  | `Eof -> return @@ Error `Eof
