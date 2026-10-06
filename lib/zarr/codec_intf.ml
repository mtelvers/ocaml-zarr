(** Codec interface types and codec-chain application for Zarr v3.

    This module sits below the individual codecs (so that the sharding codec
    can apply its inner chain) and below {!Codec}, which adds parsing and
    chain construction on top. *)

open Ztypes

module D = Ztypes.Dtype

(** Array-to-array codec operations *)
type array_to_array = {
  encode : Ndarray.t -> Ndarray.t;
  decode : Ndarray.t -> Ndarray.t;
  compute_output_shape : int array -> int array;
}

(** Array-to-bytes codec operations. [decode] may raise on malformed input;
    {!Codec.decode} turns exceptions into [`Codec_error]. *)
type array_to_bytes = {
  encode : Ndarray.t -> bytes;
  decode : int array -> D.t -> bytes -> Ndarray.t;
}

(** Bytes-to-bytes codec operations *)
type bytes_to_bytes = {
  encode : bytes -> bytes;
  decode : bytes -> bytes result;
  compute_encoded_size : int -> int option;  (** [None] if the size is variable *)
}

(** A complete codec chain *)
type codec_chain = {
  array_to_array : array_to_array list;
  array_to_bytes : array_to_bytes;
  bytes_to_bytes : bytes_to_bytes list;
}

(** Options controlling how chunks are encoded. *)
type config = {
  write_empty_chunks : bool;
  (** When [false] (the default, matching zarr-python) a chunk whose elements
      all equal the fill value is not stored: the sharding codec leaves such
      inner chunks out of the shard, and arrays delete the chunk key instead
      of writing it. *)
  domains : int;
  (** Number of OCaml domains used to encode and decode the inner chunks of a
      shard in parallel. [1] (the default) runs sequentially on the calling
      domain. Requires every inner codec to be safe to run concurrently; all
      built-in codecs and the Blosc codec are. *)
}

let default_config = { write_empty_chunks = false; domains = 1 }

(** {1 Chain application} *)

exception Decode_error of string

let decode_error_message = function
  | `Codec_error msg -> msg
  | `Checksum_mismatch -> "checksum mismatch"
  | `Invalid_metadata msg | `Unsupported_dtype msg | `Store_error msg
  | `Not_found msg | `Invalid_slice msg | `Invalid_chunk_coords msg
  | `Shape_mismatch msg -> msg

(** Encode the bytes stage of a chain only. *)
let encode_bytes chain buf =
  List.fold_left (fun b (codec : bytes_to_bytes) -> codec.encode b) buf chain.bytes_to_bytes

(** Decode the bytes stage of a chain only.
    @raise Decode_error if a codec rejects its input. *)
let decode_bytes chain buf =
  List.fold_right (fun (codec : bytes_to_bytes) b ->
    match codec.decode b with
    | Ok decoded -> decoded
    | Error e -> raise (Decode_error (decode_error_message e))
  ) chain.bytes_to_bytes buf

(** Encode an array through a whole chain. *)
let encode_chain chain arr =
  let arr = List.fold_left (fun a (codec : array_to_array) -> codec.encode a) arr chain.array_to_array in
  encode_bytes chain (chain.array_to_bytes.encode arr)

(** Decode bytes through a whole chain to an array of [shape] and [dtype].
    @raise Decode_error if a bytes codec rejects its input; array codecs may
    raise their own exceptions. *)
let decode_chain chain shape dtype bytes =
  let bytes = decode_bytes chain bytes in
  let intermediate_shape =
    List.fold_left (fun s (codec : array_to_array) -> codec.compute_output_shape s)
      shape chain.array_to_array
  in
  let arr = chain.array_to_bytes.decode intermediate_shape dtype bytes in
  List.fold_right (fun (codec : array_to_array) a -> codec.decode a) chain.array_to_array arr
