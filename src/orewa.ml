open Core
open Async

module Resp = Resp

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
  | '$' ->
    (match crlf_ends_at iobuf with
    | Some end_index ->
      let length_read = Iobuf.Consume.stringo ~len:(Int.succ end_index) iobuf in
      let length = Scanf.sscanf length_read "$%d\r\n" Fn.id in
      Log.Global.error "LEN %d" length;
      let content = Iobuf.Consume.stringo ~len:length iobuf in
      let _garbage = Iobuf.Consume.stringo ~len:2 iobuf in
      Log.Global.error "CONTENT: %s" content;
      return @@ `Stop (Resp.Bulk content)
    | None ->
      return `Continue)
  | _ -> return @@ `Stop Resp.Null

let read_resp reader =
  let%bind res = Reader.read_one_iobuf_at_a_time reader ~handle_chunk in
  match res with
  | `Eof -> return @@ Error `Eof
  | `Stopped v -> return @@ Ok v
  | `Eof_with_unconsumed_data _data -> return @@ Error `Connection_closed

let echo { reader; writer } message =
  let request = Resp.Array [Resp.Bulk "ECHO"; Resp.Bulk message] in
  let request = Resp.encode request in
  Writer.write writer request;
  read_resp reader

let init reader writer =
  { reader; writer }

let connect ?(port=6379) ~host f =
  let where = Tcp.Where_to_connect.of_host_and_port @@ Host_and_port.create ~host ~port  in
  Tcp.with_connection where @@ fun _socket reader writer ->
    let t = init reader writer in
    f t
