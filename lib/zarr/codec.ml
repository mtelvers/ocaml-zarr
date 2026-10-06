(** Codec specifications, chain construction and chunk encoding for Zarr v3 *)

open Ztypes

module D = Ztypes.Dtype
module E = Ztypes.Endianness
module IL = Ztypes.Index_location

type array_to_array = Codec_intf.array_to_array
type array_to_bytes = Codec_intf.array_to_bytes
type bytes_to_bytes = Codec_intf.bytes_to_bytes
type codec_chain = Codec_intf.codec_chain

(** Encoding options; see {!Codec_intf.config}. *)
type config = Codec_intf.config = {
  write_empty_chunks : bool;
  domains : int;
}

let default_config = Codec_intf.default_config

(** A built codec, classified by the stage of the chain it belongs to. *)
type codec_class =
  | ArrayToArray of array_to_array
  | ArrayToBytes of array_to_bytes
  | BytesToBytes of bytes_to_bytes

let ( let* ) = Result.bind

(** Build one codec from its specification. [chunk_shape] is the shape of the
    array the codec receives, after any preceding array-to-array codecs. *)
let rec build_codec ~config ~fill_value spec dtype chunk_shape =
  match spec with
  | Bytes { endian } ->
    Ok (ArrayToBytes (Codecs.Bytes_codec.create (Option.value endian ~default:E.Little)))
  | Transpose { order } -> Ok (ArrayToArray (Codecs.Transpose.create order))
  | Gzip { level } -> Ok (BytesToBytes (Codecs.Gzip.create level))
  | Zstd { level; checksum = _ } -> Ok (BytesToBytes (Codecs.Zstd.create level))
  | Crc32c -> Ok (BytesToBytes (Codecs.Crc32c.create ()))
  | Sharding { chunk_shape = inner_chunk_shape; codecs; index_codecs; index_location } ->
    let* () = Codecs.Sharding.validate ~outer_chunk_shape:chunk_shape ~inner_chunk_shape in
    let num_inner_chunks = Codecs.Sharding.total_inner_chunks chunk_shape inner_chunk_shape in
    let* inner_chain = build_chain ~config ?fill_value codecs dtype inner_chunk_shape in
    let* index_chain = build_chain ~config index_codecs D.Uint64 [| num_inner_chunks * 2 |] in
    Ok (ArrayToBytes (Codecs.Sharding.create_with_chains ~config ?fill_value
          ~outer_chunk_shape:chunk_shape ~inner_chunk_shape ~inner_chain ~index_chain
          ~index_location ~dtype ()))
  | Extension { name; config = ext_config } ->
    match Codec_registry.find name with
    | None -> Error (`Codec_error ("unknown extension codec: " ^ name))
    | Some builder ->
      let* codec = builder ext_config dtype chunk_shape in
      Ok (match codec with
          | Codec_registry.ArrayToArray c -> ArrayToArray c
          | Codec_registry.ArrayToBytes c -> ArrayToBytes c
          | Codec_registry.BytesToBytes c -> BytesToBytes c)

(** Build a codec chain from a list of specifications, checking the
    array-to-array / array-to-bytes / bytes-to-bytes ordering.

    [fill_value] lets the sharding codec omit inner chunks that are entirely
    fill value; pass the array's fill value when building a chain for real
    chunks. *)
and build_chain ?(config = default_config) ?fill_value specs dtype chunk_shape =
  let step (a2a, a2b, b2b, shape) spec =
    let* codec = build_codec ~config ~fill_value spec dtype shape in
    match codec, a2b with
    | ArrayToArray c, None -> Ok (c :: a2a, a2b, b2b, c.compute_output_shape shape)
    | ArrayToArray _, Some _ ->
      Error (`Codec_error "invalid codec ordering: array-to-array codec after array-to-bytes")
    | ArrayToBytes c, None -> Ok (a2a, Some c, b2b, shape)
    | ArrayToBytes _, Some _ ->
      Error (`Codec_error "invalid codec ordering: multiple array-to-bytes codecs")
    | BytesToBytes c, Some _ -> Ok (a2a, a2b, c :: b2b, shape)
    | BytesToBytes _, None ->
      Error (`Codec_error "invalid codec ordering: bytes-to-bytes codec before array-to-bytes")
  in
  let* a2a, a2b, b2b, _ =
    List.fold_left (fun acc spec -> let* acc = acc in step acc spec)
      (Ok ([], None, [], chunk_shape)) specs
  in
  match a2b with
  | None -> Error (`Codec_error "codec chain must contain exactly one array->bytes codec")
  | Some array_to_bytes ->
    Ok { Codec_intf.array_to_array = List.rev a2a; array_to_bytes; bytes_to_bytes = List.rev b2b }

(** Encode an array through the chain. *)
let encode = Codec_intf.encode_chain

(** Decode bytes through the chain to an array of [shape] and [dtype]. *)
let decode chain shape dtype bytes =
  match Codec_intf.decode_chain chain shape dtype bytes with
  | arr -> Ok arr
  | exception Codec_intf.Decode_error msg -> Error (`Codec_error msg)
  | exception Failure msg -> Error (`Codec_error msg)
  | exception Invalid_argument msg -> Error (`Codec_error msg)
  | exception exn -> Error (`Codec_error ("decode error: " ^ Printexc.to_string exn))

(** {1 JSON} *)

let int_array_of_json json =
  Yojson.Safe.Util.(json |> to_list |> List.map to_int |> Array.of_list)

let int_array_to_json a = `List (Array.to_list (Array.map (fun i -> `Int i) a))

(** Parse codec specifications from their JSON list. *)
let rec specs_of_json json_list =
  let open Yojson.Safe.Util in
  let parse_one json =
    let name = json |> member "name" |> to_string in
    let config = match json |> member "configuration" with `Null -> `Assoc [] | c -> c in
    match name with
    | "bytes" ->
      let endian = match config |> member "endian" |> to_string_option with
        | Some "little" -> Some E.Little
        | Some "big" -> Some E.Big
        | _ -> None
      in
      Ok (Bytes { endian })
    | "transpose" -> Ok (Transpose { order = int_array_of_json (config |> member "order") })
    | "gzip" ->
      Ok (Gzip { level = config |> member "level" |> to_int_option |> Option.value ~default:5 })
    | "zstd" ->
      let level = config |> member "level" |> to_int_option |> Option.value ~default:3 in
      let checksum = config |> member "checksum" |> to_bool_option |> Option.value ~default:false in
      Ok (Zstd { level; checksum })
    | "crc32c" -> Ok Crc32c
    | "sharding_indexed" ->
      let chunk_shape = int_array_of_json (config |> member "chunk_shape") in
      let* codecs = specs_of_json (config |> member "codecs" |> to_list) in
      let* index_codecs = specs_of_json (config |> member "index_codecs" |> to_list) in
      let index_location = match config |> member "index_location" |> to_string_option with
        | Some "start" -> IL.Start
        | _ -> IL.End
      in
      Ok (Sharding { chunk_shape; codecs; index_codecs; index_location })
    | name when Codec_registry.is_registered name -> Ok (Extension { name; config })
    | name -> Error (`Codec_error ("unsupported codec: " ^ name))
  in
  List.fold_left (fun acc json ->
    let* acc = acc in
    let* spec = parse_one json in
    Ok (spec :: acc)
  ) (Ok []) json_list
  |> Result.map List.rev

(** Convert codec specifications to their JSON list. *)
let rec specs_to_json specs = List.map spec_to_json specs

and spec_to_json spec =
  let named name configuration = `Assoc [ ("name", `String name); ("configuration", configuration) ] in
  match spec with
  | Bytes { endian } ->
    let endian = match endian with Some E.Big -> "big" | Some E.Little | None -> "little" in
    named "bytes" (`Assoc [ ("endian", `String endian) ])
  | Transpose { order } -> named "transpose" (`Assoc [ ("order", int_array_to_json order) ])
  | Gzip { level } -> named "gzip" (`Assoc [ ("level", `Int level) ])
  | Zstd { level; checksum } ->
    named "zstd" (`Assoc [ ("level", `Int level); ("checksum", `Bool checksum) ])
  | Crc32c -> named "crc32c" (`Assoc [])
  | Sharding { chunk_shape; codecs; index_codecs; index_location } ->
    named "sharding_indexed" (`Assoc [
      ("chunk_shape", int_array_to_json chunk_shape);
      ("codecs", `List (specs_to_json codecs));
      ("index_codecs", `List (specs_to_json index_codecs));
      ("index_location", `String (match index_location with IL.Start -> "start" | IL.End -> "end"));
    ])
  | Extension { name; config } -> named name config
