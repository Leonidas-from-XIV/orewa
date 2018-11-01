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

let peek_record ~len iobuf =
  Iobuf.Peek.stringo ~len ~pos:0 iobuf
  |> String.subo ~len:(len - 2)

let discard_prefix =
  String.subo ~pos:1

type consume = int

type elements = int

type nested_resp = | Element of consume * Resp.t | Array of (elements * consume * nested_resp Stack.t)

let rec one_record_read stack e =
  match Stack.top stack with
  | None
  | Some Element _
  | Some Array (_, 0, _) ->
    Stack.push stack e
  | Some Array (left_to_read, consume, inner_stack) ->
    let _ = Stack.pop stack in
    one_record_read inner_stack e;
    let appended = Array (Int.pred left_to_read, consume, inner_stack) in
    Stack.push stack appended

let finished_array_read stack =
  match Stack.top stack with
  | Some Array (0, _, _) -> true
  | _ -> false

let rec unwind_stack stack =
  stack
  |> Stack.to_list
  |> List.fold ~init:(0, []) ~f:(fun (consumed, unwound) ->
      function
      | Element (consume, resp) -> (consume + consumed, resp :: unwound)
      | Array (_, consume, stack) ->
        let consumed', unwound' = unwind_stack stack in
        (consume + consumed' + consumed, (Resp.Array unwound')::unwound))
  |> Tuple2.map_snd ~f:List.rev

let rec handle_chunk stack iobuf =
  match record_length iobuf with
  | None -> return `Continue
  | Some len ->
    (* peek, because if `Continue is returned we need to preserve the prefix and
     * don't consume it *)
    match Iobuf.Peek.char ~pos:0 iobuf with
    | '+' ->
      (* Simple string *)
      let content = peek_record ~len iobuf |> discard_prefix in
      Log.Global.error "READ: %s" (String.escaped content);
      Iobuf.advance iobuf len;
      let resp = Resp.String content in
      one_record_read stack (Element (len, resp));
      return @@ `Stop resp
    | '-' ->
      (* Error, which is also a simple string *)
      let content = consume_record ~len iobuf |> discard_prefix in
      Log.Global.error "READ: %s" (String.escaped content);
      return @@ `Stop (Resp.Error content)
    | '$' ->
      (* Bulk string *)
      let len = consume_record ~len iobuf |> discard_prefix |> int_of_string in
      Log.Global.error "LEN %d" len;
      (* read trailing \r\n and discard *)
      let content = consume_record ~len:(len + 2) iobuf in
      Log.Global.error "CONTENT: '%s'" (String.escaped content);
      return @@ `Stop (Resp.Bulk content)
    | ':' ->
      (* Integer *)
      let value = consume_record ~len iobuf |> discard_prefix |> int_of_string in
      return @@ `Stop (Resp.Integer value)
    | '*' ->
      (* Array *)
      (* There is a good chance that if one of the calls emits `Continue the
       * code will be incorrect, since we consumed from the iobuf but discard
       * whatever we have consumed and parsed so far by emitting `Continue *)
        (let elements = consume_record ~len iobuf |> discard_prefix |> int_of_string in
        Log.Global.error "ELEMENTS TO READ: %d" elements;
        let rec loop xs = function
          | 0 -> return @@ `Stop xs
          | remaining ->
            match%bind handle_chunk stack iobuf with
            | `Stop parsed -> loop (parsed::xs) (Int.pred remaining)
            | `Continue -> return `Continue
        in
        match%bind loop [] elements with
        | `Continue -> return `Continue
        | `Stop xs -> return @@ `Stop (Resp.Array xs))
    | unknown ->
      (* Unknown match *)
      Log.Global.error "Unparseable type tag %C" unknown;
      return @@ `Stop Resp.Null

type resp_list = Resp.t list [@@deriving show]

let read_resp reader =
  let stack = Stack.create () in
  let%bind res = Reader.read_one_iobuf_at_a_time reader ~handle_chunk:(handle_chunk stack) in
  let consume, resps = unwind_stack stack in
  Log.Global.error "Consume %d from Iobuf, show %s" consume (show_resp_list resps);
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
