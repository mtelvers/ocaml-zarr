(** Array handles and chunked read/write operations for Zarr v3 *)

module FV_mod = Fill_value

open Ztypes

module E = Ztypes.Endianness

(** An open array: its store, path, metadata and the codec chain built from
    the metadata. *)
type 'store t = {
  store : 'store;
  path : string;
  metadata : array_metadata;
  codec_chain : Codec.codec_chain;
  config : Codec.config;
}

(** The store operations arrays need. *)
module type STORE_OPS = sig
  type t
  val get : t -> string -> bytes option
  val set : t -> string -> bytes -> unit
  val erase : t -> string -> unit
  val erase_prefix : t -> string -> unit
  val exists : t -> string -> bool
end

module Make (S : STORE_OPS) = struct
  type store = S.t
  type nonrec t = S.t t

  let ( let* ) = Result.bind

  let write_metadata store path metadata =
    S.set store (Chunk_key.metadata_path path) (Bytes.of_string (Metadata.array_to_json metadata))

  (** Create an array, writing its metadata.  [config] controls empty-chunk
      omission and parallel shard encoding; see {!Codec.config}. *)
  let create ?(config = Codec.default_config) store ~path ~shape ~chunks ~dtype
      ?fill_value ?codecs () =
    let fill_value = Option.value fill_value ~default:(FV_mod.default dtype) in
    let codecs = Option.value codecs ~default:[ Bytes { endian = Some E.Little } ] in
    let metadata = Metadata.create_array_metadata ~shape ~chunks ~dtype ~fill_value ~codecs () in
    let* codec_chain = Codec.build_chain ~config ~fill_value codecs dtype chunks in
    write_metadata store path metadata;
    Ok { store; path; metadata; codec_chain; config }

  (** Open an existing array. *)
  let open_ ?(config = Codec.default_config) store ~path =
    match S.get store (Chunk_key.metadata_path path) with
    | None -> Error (`Not_found ("array not found: " ^ path))
    | Some meta_bytes ->
      let* metadata = Metadata.array_of_json (Bytes.to_string meta_bytes) in
      let chunks = Chunk_grid.chunk_shape metadata.chunk_grid in
      let* codec_chain =
        Codec.build_chain ~config ~fill_value:metadata.fill_value
          metadata.codecs metadata.data_type chunks
      in
      Ok { store; path; metadata; codec_chain; config }

  let metadata arr = arr.metadata
  let shape arr = arr.metadata.shape
  let dtype arr = arr.metadata.data_type
  let chunks arr = Chunk_grid.chunk_shape arr.metadata.chunk_grid
  let fill_value arr = arr.metadata.fill_value

  (** {1 Chunks}

      Chunks are always encoded at the full chunk shape, also at the array's
      edges, as the specification and zarr-python do; elements beyond the
      array shape hold the fill value. *)

  let chunk_key arr coords = Chunk_key.full_path arr.path arr.metadata.chunk_key_encoding coords

  let decode_chunk arr bytes =
    match Codec.decode arr.codec_chain (chunks arr) arr.metadata.data_type bytes with
    | Ok chunk -> chunk
    | Error e -> failwith ("chunk decode error: " ^ Codec_intf.decode_error_message e)

  (** The stored chunk at [coords], or [None] if it has not been written. *)
  let read_chunk arr coords = Option.map (decode_chunk arr) (S.get arr.store (chunk_key arr coords))

  (** The chunk at [coords], filled with the fill value if it is not stored. *)
  let get_chunk arr coords =
    match read_chunk arr coords with
    | Some chunk -> chunk
    | None -> Ndarray.make arr.metadata.data_type (chunks arr) arr.metadata.fill_value

  (** Store a chunk.  Unless [config.write_empty_chunks] is set, a chunk that
      is entirely fill value is deleted rather than written. *)
  let set_chunk arr coords data =
    let key = chunk_key arr coords in
    if (not arr.config.write_empty_chunks) && Ndarray.equals_fill data arr.metadata.fill_value
    then S.erase arr.store key
    else S.set arr.store key (Codec.encode arr.codec_chain data)

  (** {1 Elements} *)

  let local_index arr idx = Array.mapi (fun i v -> v mod (chunks arr).(i)) idx

  let get arr idx =
    let coords = Chunk_grid.chunk_for_index arr.metadata.chunk_grid idx in
    match read_chunk arr coords with
    | Some chunk -> Ndarray.get chunk (local_index arr idx)
    | None ->
      let one = Ndarray.make arr.metadata.data_type (Array.make (Array.length idx) 1) arr.metadata.fill_value in
      Ndarray.get one (Array.make (Array.length idx) 0)

  let set arr idx value =
    let coords = Chunk_grid.chunk_for_index arr.metadata.chunk_grid idx in
    let chunk = get_chunk arr coords in
    Ndarray.set chunk (local_index arr idx) value;
    set_chunk arr coords chunk

  (** {1 Slices} *)

  let ranges_exn arr slices =
    match Indexing.normalize arr.metadata.shape slices with
    | Ok ranges -> ranges
    | Error (`Invalid_slice msg) -> invalid_arg ("invalid slice: " ^ msg)

  (* The chunks a selection touches, each with its per-axis overlap. *)
  let touched_chunks arr ranges =
    let chunk_shape = chunks arr and array_shape = arr.metadata.shape in
    Indexing.chunks_for_ranges ~chunk_shape ~array_shape ranges
    |> List.map (fun coords -> (coords, Indexing.chunk_overlap ~chunk_shape ~array_shape coords ranges))

  let unit_steps = Array.for_all (fun (o : Indexing.overlap) -> o.step = 1)

  (** Read a selection.  Missing chunks read as the fill value.
      Raises [Invalid_argument] if the slices are invalid for the array. *)
  let get_slice arr slices =
    let ranges = ranges_exn arr slices in
    let result =
      Ndarray.make arr.metadata.data_type (Indexing.shape_of_ranges ranges) arr.metadata.fill_value
    in
    List.iter (fun (coords, overlap) ->
      match read_chunk arr coords with
      | None -> ()  (* already fill value *)
      | Some chunk ->
        let src_offset = Array.map (fun (o : Indexing.overlap) -> o.chunk_offset) overlap in
        let dst_offset = Array.map (fun (o : Indexing.overlap) -> o.out_offset) overlap in
        let shape = Array.map (fun (o : Indexing.overlap) -> o.count) overlap in
        if unit_steps overlap then
          Ndarray.blit ~src:chunk ~src_offset ~dst:result ~dst_offset ~shape
        else
          Ndarray.blit_strided ~src:chunk ~src_offset
            ~src_step:(Array.map (fun (o : Indexing.overlap) -> o.step) overlap)
            ~dst:result ~dst_offset ~shape
    ) (touched_chunks arr ranges);
    result

  (** Write [data] to a selection; its shape must match the selection's.
      A chunk the selection covers entirely is overwritten without being
      read first.
      Raises [Invalid_argument] if the slices are invalid or the shape differs. *)
  let set_slice arr slices data =
    let ranges = ranges_exn arr slices in
    let selection_shape = Indexing.shape_of_ranges ranges in
    if Ndarray.shape data <> selection_shape then
      invalid_arg (Printf.sprintf "set_slice: data has shape [%s] but the selection has shape [%s]"
        (String.concat ";" (Array.to_list (Array.map string_of_int (Ndarray.shape data))))
        (String.concat ";" (Array.to_list (Array.map string_of_int selection_shape))));
    let chunk_shape = chunks arr in
    List.iter (fun (coords, overlap) ->
      let dst_offset = Array.map (fun (o : Indexing.overlap) -> o.chunk_offset) overlap in
      let src_offset = Array.map (fun (o : Indexing.overlap) -> o.out_offset) overlap in
      let shape = Array.map (fun (o : Indexing.overlap) -> o.count) overlap in
      let covers_chunk = unit_steps overlap && shape = chunk_shape in
      let chunk =
        if covers_chunk then Ndarray.empty arr.metadata.data_type chunk_shape
        else get_chunk arr coords
      in
      if unit_steps overlap then
        Ndarray.blit ~src:data ~src_offset ~dst:chunk ~dst_offset ~shape
      else
        Ndarray.scatter_strided ~src:data ~src_offset ~dst:chunk ~dst_offset
          ~dst_step:(Array.map (fun (o : Indexing.overlap) -> o.step) overlap) ~shape;
      set_chunk arr coords chunk
    ) (touched_chunks arr ranges)

  (** {1 Attributes and lifecycle} *)

  let attrs arr = Option.value ~default:`Null arr.metadata.attributes

  (** Replace the attributes, rewriting the metadata. *)
  let set_attrs arr new_attrs =
    write_metadata arr.store arr.path { arr.metadata with attributes = Some new_attrs }

  (** Delete the array's metadata and every chunk. *)
  let delete arr =
    S.erase arr.store (Chunk_key.metadata_path arr.path);
    S.erase_prefix arr.store (if arr.path = "" || arr.path = "/" then "" else arr.path ^ "/")
end
