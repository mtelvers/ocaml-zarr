(** Codec interface and pipeline for Zarr v3 *)

open Ztypes

module D = Ztypes.Dtype
module E = Ztypes.Endianness
module IL = Ztypes.Index_location

(* Re-export types from Codec_intf *)
type array_to_array = Codec_intf.array_to_array
type array_to_bytes = Codec_intf.array_to_bytes
type bytes_to_bytes = Codec_intf.bytes_to_bytes
type codec_chain = Codec_intf.codec_chain

(** Classify a codec specification *)
type codec_class =
  | ArrayToArray of array_to_array
  | ArrayToBytes of array_to_bytes
  | BytesToBytes of bytes_to_bytes

(** Forward declaration for recursive chain building *)
let build_chain_ref : (codec_spec list -> D.t -> int array -> codec_chain result) ref =
  ref (fun _ _ _ -> Error (`Codec_error "not initialized"))

(** Build an individual codec from its specification *)
let build_codec spec dtype chunk_shape =
  match spec with
  | Bytes { endian } ->
    let endian = match endian with
      | Some e -> e
      | None -> E.Little  (* Default to little endian *)
    in
    ArrayToBytes (Codecs.Bytes_codec.create endian)

  | Transpose { order } ->
    ArrayToArray (Codecs.Transpose.create order)

  | Gzip { level } ->
    BytesToBytes (Codecs.Gzip.create level)

  | Zstd { level; checksum = _ } ->
    BytesToBytes (Codecs.Zstd.create level)

  | Crc32c ->
    BytesToBytes (Codecs.Crc32c.create ())

  | Sharding { chunk_shape = inner_chunk_shape; codecs; index_codecs; index_location } ->
    (* Build inner codec chains *)
    let num_inner_chunks = Codecs.Sharding.total_inner_chunks chunk_shape inner_chunk_shape in
    let inner_chain_result = !build_chain_ref codecs dtype inner_chunk_shape in
    let index_chain_result = !build_chain_ref index_codecs D.Uint64 [|num_inner_chunks * 2|] in

    (match inner_chain_result, index_chain_result with
     | Ok inner_chain, Ok index_chain ->
       ArrayToBytes (Codecs.Sharding.create_with_chains
         ~outer_chunk_shape:chunk_shape
         ~inner_chunk_shape
         ~inner_chain
         ~index_chain
         ~index_location
         ~dtype)
     | Error (`Codec_error msg), _ ->
       failwith ("sharding inner codec chain error: " ^ msg)
     | Error _, _ ->
       failwith "sharding inner codec chain error"
     | _, Error (`Codec_error msg) ->
       failwith ("sharding index codec chain error: " ^ msg)
     | _, Error _ ->
       failwith "sharding index codec chain error")

  | Extension { name; config } ->
    (match Codec_registry.find name with
     | Some builder ->
       (match builder config dtype chunk_shape with
        | Ok (Codec_registry.ArrayToArray c) -> ArrayToArray c
        | Ok (Codec_registry.ArrayToBytes c) -> ArrayToBytes c
        | Ok (Codec_registry.BytesToBytes c) -> BytesToBytes c
        | Error (`Codec_error msg) -> failwith ("extension codec '" ^ name ^ "' error: " ^ msg)
        | Error _ -> failwith ("extension codec '" ^ name ^ "' error"))
     | None ->
       failwith ("unknown extension codec: " ^ name))

(** Build a codec chain from a list of codec specifications *)
let build_chain specs dtype chunk_shape =
  let array_to_array = ref [] in
  let array_to_bytes = ref None in
  let bytes_to_bytes = ref [] in

  let current_shape = ref chunk_shape in

  let error = ref None in

  List.iter (fun spec ->
    if Option.is_none !error then
      match (try Ok (build_codec spec dtype !current_shape) with Failure msg -> Error msg) with
      | Error msg ->
        error := Some (`Codec_error ("failed to build codec: " ^ msg))
      | Ok (ArrayToArray codec) ->
        if Option.is_some !array_to_bytes then
          error := Some (`Codec_error "invalid codec ordering: array-to-array codec after array-to-bytes")
        else begin
          array_to_array := codec :: !array_to_array;
          current_shape := codec.compute_output_shape !current_shape
        end
      | Ok (ArrayToBytes codec) ->
        if Option.is_some !array_to_bytes then
          error := Some (`Codec_error "invalid codec ordering: multiple array-to-bytes codecs")
        else
          array_to_bytes := Some codec
      | Ok (BytesToBytes codec) ->
        if Option.is_none !array_to_bytes then
          error := Some (`Codec_error "invalid codec ordering: bytes-to-bytes codec before array-to-bytes")
        else
          bytes_to_bytes := codec :: !bytes_to_bytes
  ) specs;

  match !error with
  | Some e -> Error e
  | None ->
    match !array_to_bytes with
    | None -> Error (`Codec_error "codec chain must contain exactly one array->bytes codec")
    | Some a2b ->
      Ok {
        Codec_intf.array_to_array = List.rev !array_to_array;
        array_to_bytes = a2b;
        bytes_to_bytes = List.rev !bytes_to_bytes;
      }

(* Initialize the forward reference *)
let () = build_chain_ref := build_chain

(** Encode an ndarray through the codec chain *)
let encode (chain : codec_chain) arr =
  (* Apply array-to-array codecs *)
  let arr = List.fold_left (fun a (codec : array_to_array) ->
    codec.encode a
  ) arr chain.array_to_array in

  (* Apply array-to-bytes codec *)
  let bytes = chain.array_to_bytes.encode arr in

  (* Apply bytes-to-bytes codecs *)
  List.fold_left (fun b (codec : bytes_to_bytes) ->
    codec.encode b
  ) bytes chain.bytes_to_bytes

(** Decode bytes through the codec chain to an ndarray.
    Returns [Ok ndarray] on success or [Error _] on failure. *)
let decode (chain : codec_chain) shape dtype bytes =
  try
    (* Apply bytes-to-bytes codecs in reverse *)
    let bytes = List.fold_right (fun (codec : bytes_to_bytes) b ->
      match codec.decode b with
      | Ok decoded -> decoded
      | Error (`Codec_error msg) -> failwith ("bytes-to-bytes decode error: " ^ msg)
      | Error `Checksum_mismatch -> failwith "checksum mismatch"
      | Error _ -> failwith "bytes-to-bytes decode error"
    ) chain.bytes_to_bytes bytes in

    (* Calculate intermediate shape after a2a codecs *)
    let intermediate_shape = List.fold_left (fun s (codec : array_to_array) ->
      codec.compute_output_shape s
    ) shape chain.array_to_array in

    (* Apply array-to-bytes codec *)
    let arr = chain.array_to_bytes.decode intermediate_shape dtype bytes in

    (* Apply array-to-array codecs in reverse *)
    let arr = List.fold_right (fun (codec : array_to_array) a ->
      codec.decode a
    ) chain.array_to_array arr in

    Ok arr
  with
  | Failure msg -> Error (`Codec_error msg)
  | exn -> Error (`Codec_error ("decode error: " ^ Printexc.to_string exn))

(** Parse codec specifications from JSON *)
let rec specs_of_json json_list =
  let open Yojson.Safe.Util in
  let parse_one json =
    let name = json |> member "name" |> to_string in
    let config = json |> member "configuration" in
    match name with
    | "bytes" ->
      let endian = match config |> member "endian" |> to_string_option with
        | Some "little" -> Some E.Little
        | Some "big" -> Some E.Big
        | _ -> None
      in
      Ok (Bytes { endian })

    | "transpose" ->
      let order = config |> member "order" |> to_list |> List.map to_int |> Array.of_list in
      Ok (Transpose { order })

    | "gzip" ->
      let level = config |> member "level" |> to_int_option |> Option.value ~default:5 in
      Ok (Gzip { level })

    | "zstd" ->
      let level = config |> member "level" |> to_int_option |> Option.value ~default:3 in
      let checksum = config |> member "checksum" |> to_bool_option |> Option.value ~default:false in
      Ok (Zstd { level; checksum })

    | "crc32c" ->
      Ok Crc32c

    | "sharding_indexed" ->
      let chunk_shape = config |> member "chunk_shape" |> to_list |> List.map to_int |> Array.of_list in
      let codecs_json = config |> member "codecs" |> to_list in
      let index_codecs_json = config |> member "index_codecs" |> to_list in
      let index_location = match config |> member "index_location" |> to_string_option with
        | Some "start" -> IL.Start
        | _ -> IL.End
      in
      (match specs_of_json codecs_json, specs_of_json index_codecs_json with
       | Ok codecs, Ok index_codecs ->
         Ok (Sharding { chunk_shape; codecs; index_codecs; index_location })
       | Error e, _ -> Error e
       | _, Error e -> Error e)

    | name ->
      if Codec_registry.is_registered name then
        let config = if config = `Null then `Assoc [] else config in
        Ok (Extension { name; config })
      else
        Error (`Codec_error ("unsupported codec: " ^ name))
  in
  let rec parse_all acc = function
    | [] -> Ok (List.rev acc)
    | json :: rest ->
      match parse_one json with
      | Ok spec -> parse_all (spec :: acc) rest
      | Error e -> Error e
  in
  parse_all [] json_list

(** Convert codec specifications to JSON *)
let rec specs_to_json specs =
  List.map spec_to_json specs

and spec_to_json = function
  | Bytes { endian } ->
    let endian_str = match endian with
      | Some E.Little -> "little"
      | Some E.Big -> "big"
      | None -> "little"
    in
    `Assoc [
      ("name", `String "bytes");
      ("configuration", `Assoc [("endian", `String endian_str)])
    ]

  | Transpose { order } ->
    `Assoc [
      ("name", `String "transpose");
      ("configuration", `Assoc [
        ("order", `List (Array.to_list (Array.map (fun i -> `Int i) order)))
      ])
    ]

  | Gzip { level } ->
    `Assoc [
      ("name", `String "gzip");
      ("configuration", `Assoc [("level", `Int level)])
    ]

  | Zstd { level; checksum } ->
    `Assoc [
      ("name", `String "zstd");
      ("configuration", `Assoc [
        ("level", `Int level);
        ("checksum", `Bool checksum)
      ])
    ]

  | Crc32c ->
    `Assoc [
      ("name", `String "crc32c");
      ("configuration", `Assoc [])
    ]

  | Sharding { chunk_shape; codecs; index_codecs; index_location } ->
    `Assoc [
      ("name", `String "sharding_indexed");
      ("configuration", `Assoc [
        ("chunk_shape", `List (Array.to_list (Array.map (fun i -> `Int i) chunk_shape)));
        ("codecs", `List (specs_to_json codecs));
        ("index_codecs", `List (specs_to_json index_codecs));
        ("index_location", `String (match index_location with IL.Start -> "start" | IL.End -> "end"))
      ])
    ]

  | Extension { name; config } ->
    `Assoc [
      ("name", `String name);
      ("configuration", config)
    ]
