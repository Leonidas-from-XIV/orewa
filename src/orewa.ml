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

let discard_prefix =
  String.subo ~pos:1

type nested_resp = | Element of Resp.t | Array of (int * nested_resp Stack.t)

let show_nested_resp = function
  | Element _ -> "Element(..)"
  | Array (to_read, _) -> Printf.sprintf "Array[%d](..)" to_read

let rec one_record_read stack e =
  match Stack.top stack with
  | None
  | Some Element _
  | Some Array (0, _) ->
    Stack.push stack e
  | Some Array (left_to_read, inner_stack) ->
    let _ = Stack.pop stack in
    one_record_read inner_stack e;
    let appended = Array (Int.pred left_to_read, inner_stack) in
    Stack.push stack appended

let rec unwind_stack stack =
  stack
  |> Stack.to_list
  |> List.map ~f:(function
    | Element resp -> resp
    | Array (_, stack) ->
      Resp.Array (List.rev (unwind_stack stack)))

let unfinished_array stack =
  match Stack.top stack with
  | Some Array (n, _) when n > 0 -> true
  | _ -> false

let rec handle_chunk stack iobuf =
  let read_on stack =
    match unfinished_array stack with
    | true -> handle_chunk stack iobuf
    | false -> return @@ `Stop ()
  in
  match record_length iobuf with
  | None -> return `Continue
  | Some len ->
    (* peek, because if `Continue is returned we need to preserve the prefix and
     * don't consume it *)
    match Iobuf.Peek.char ~pos:0 iobuf with
    | '+' ->
      (* Simple string *)
      let content = consume_record ~len iobuf |> discard_prefix in
      Log.Global.error "READ: %s" (String.escaped content);
      let resp = Resp.String content in
      one_record_read stack (Element resp);
      read_on stack
    | '-' ->
      (* Error, which is also a simple string *)
      let content = consume_record ~len iobuf |> discard_prefix in
      Log.Global.error "READ: %s" (String.escaped content);
      let resp = Resp.Error content in
      one_record_read stack (Element resp);
      read_on stack
    | '$' ->
      (* Bulk string *)
      let bulk_len = consume_record ~len iobuf |> discard_prefix |> int_of_string in
      Log.Global.error "LEN %d" bulk_len;
      (* read including the trailing \r\n and discard those *)
      let content = consume_record ~len:(bulk_len + 2) iobuf in
      Log.Global.error "CONTENT: '%s'" (String.escaped content);
      let resp = Resp.Bulk content in
      one_record_read stack (Element resp);
      read_on stack
    | ':' ->
      (* Integer *)
      let value = consume_record ~len iobuf |> discard_prefix |> int_of_string in
      let resp = Resp.Integer value in
      one_record_read stack (Element resp);
      read_on stack
    | '*' ->
      (* Array *)
      let elements = consume_record ~len iobuf |> discard_prefix |> int_of_string in
      Log.Global.error "ELEMENTS TO READ: %d" elements;
      one_record_read stack (Array (elements, Stack.create ()));
      handle_chunk stack iobuf
    | unknown ->
      (* Unknown match *)
      Log.Global.error "Unparseable type tag %C" unknown;
      return @@ `Stop ()

type resp_list = Resp.t list [@@deriving show]

let read_resp reader =
  let stack = Stack.create () in
  let%bind res = Reader.read_one_iobuf_at_a_time reader ~handle_chunk:(handle_chunk stack) in
  let resps = unwind_stack stack in
  Log.Global.error "Stack unwound to: %s" (show_resp_list resps);
  match List.hd resps with
  | None -> return @@ Error `Unexpected
  | Some resp ->
    match res with
    | `Eof -> return @@ Error `Eof
    | `Stopped () -> return @@ Ok resp
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
  let open Deferred.Result.Let_syntax in
  submit_request writer ["LRANGE"; key; string_of_int start; string_of_int stop];
  match%bind read_resp reader with
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
