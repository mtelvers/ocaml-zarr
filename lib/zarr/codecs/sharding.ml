(** Sharding codec ([sharding_indexed]): stores a grid of inner chunks, each
    encoded with its own codec chain, in one shard object followed (or
    preceded) by an index of [(offset, nbytes)] pairs. *)

module D = Ztypes.Dtype
module IL = Ztypes.Index_location

(** Marker for an absent inner chunk: offset and nbytes both [2^64 - 1]. *)
let empty_marker = Int64.minus_one

type index_entry = { offset : int64; nbytes : int64 }

let empty_entry = { offset = empty_marker; nbytes = empty_marker }

let is_empty_entry entry =
  Int64.equal entry.offset empty_marker && Int64.equal entry.nbytes empty_marker

(** Number of inner chunks along each axis of a shard. *)
let inner_chunks_per_shard outer_shape inner_shape =
  Array.mapi (fun i outer -> (outer + inner_shape.(i) - 1) / inner_shape.(i)) outer_shape

(** Total number of inner chunks in a shard. *)
let total_inner_chunks outer_shape inner_shape =
  Array.fold_left ( * ) 1 (inner_chunks_per_shard outer_shape inner_shape)

let inner_coords_to_index = Ndarray.index_to_offset
let index_to_inner_coords = Ndarray.offset_to_index

(** Check that the inner chunk shape evenly divides the shard shape, as the
    specification requires. *)
let validate ~outer_chunk_shape ~inner_chunk_shape =
  if Array.length inner_chunk_shape <> Array.length outer_chunk_shape then
    Error (`Codec_error "sharding: inner chunk_shape has a different rank from the shard shape")
  else
    match Array.to_list (Array.mapi (fun i inner -> (i, inner, outer_chunk_shape.(i))) inner_chunk_shape)
          |> List.find_opt (fun (_, inner, outer) -> inner <= 0 || outer mod inner <> 0) with
    | Some (i, inner, outer) ->
      Error (`Codec_error (Printf.sprintf
        "sharding: inner chunk shape %d does not divide shard shape %d on axis %d" inner outer i))
    | None -> Ok ()

(** The linear (C-order) indices of the inner chunks in Morton (Z-curve)
    order, which is the order zarr-python lays chunk data out in a shard.
    Following it keeps shards byte-identical with zarr-python's.  This is the
    compressed Morton code of zarr-python's [morton_order_iter]: axis 0 takes
    the least significant bit, and axes that have run out of bits are skipped. *)
let morton_order dims =
  let ndim = Array.length dims in
  let rec ceil_log2 c acc = if 1 lsl acc >= c then acc else ceil_log2 c (acc + 1) in
  let bits = Array.map (fun c -> ceil_log2 c 0) dims in
  let max_bits = Array.fold_left max 0 bits in
  let n = Array.fold_left ( * ) 1 dims in
  let order = Array.make n 0 in
  let count = ref 0 and z = ref 0 in
  let coords = Array.make ndim 0 in
  while !count < n do
    Array.fill coords 0 ndim 0;
    let input_bit = ref 0 in
    for coord_bit = 0 to max_bits - 1 do
      for dim = 0 to ndim - 1 do
        if coord_bit < bits.(dim) then begin
          coords.(dim) <- coords.(dim) lor (((!z lsr !input_bit) land 1) lsl coord_bit);
          incr input_bit
        end
      done
    done;
    if Array.for_all2 (fun c d -> c < d) coords dims then begin
      order.(!count) <- Ndarray.index_to_offset dims coords;
      incr count
    end;
    incr z
  done;
  order

(** Serialise the index and run it through the index codec chain. *)
let encode_index index_chain entries =
  let buf = Bytes.create (Array.length entries * 16) in
  Array.iteri (fun i e ->
    Bytes.set_int64_le buf (i * 16) e.offset;
    Bytes.set_int64_le buf (i * 16 + 8) e.nbytes
  ) entries;
  Codec_intf.encode_bytes index_chain buf

(** Decode an index; entries beyond the decoded data read as empty. *)
let decode_index index_chain num_inner_chunks bytes =
  let decoded = Codec_intf.decode_bytes index_chain bytes in
  let available = Bytes.length decoded / 16 in
  Array.init num_inner_chunks (fun i ->
    if i < available then
      { offset = Bytes.get_int64_le decoded (i * 16);
        nbytes = Bytes.get_int64_le decoded (i * 16 + 8) }
    else empty_entry)

(** Create a sharding codec from pre-built inner and index codec chains.

    [fill_value] enables omission of inner chunks whose elements all equal it
    (unless [config.write_empty_chunks] is set); without it every in-bounds
    inner chunk is stored.  [config.domains] > 1 encodes and decodes inner
    chunks in parallel. *)
let create_with_chains
    ?(config = Codec_intf.default_config) ?fill_value
    ~outer_chunk_shape ~inner_chunk_shape ~inner_chain ~index_chain ~index_location ~dtype () =
  let chunks_per_dim = inner_chunks_per_shard outer_chunk_shape inner_chunk_shape in
  let num_inner_chunks = Array.fold_left ( * ) 1 chunks_per_dim in
  let ndim = Array.length outer_chunk_shape in
  let zeros = Array.make ndim 0 in
  (* The index codecs must be fixed-size, so the encoded size of an all-empty
     index is the size of every index. *)
  let index_size = Bytes.length (encode_index index_chain (Array.make num_inner_chunks empty_entry)) in
  let elide_empty = (not config.write_empty_chunks) && Option.is_some fill_value in
  let layout_order = morton_order chunks_per_dim in
  let inner_start i = Array.mapi (fun d c -> c * inner_chunk_shape.(d)) (index_to_inner_coords chunks_per_dim i) in

  let encode arr =
    if Ndarray.shape arr <> outer_chunk_shape then
      invalid_arg (Printf.sprintf "sharding: expected a shard of shape [%s], got [%s]"
        (String.concat ";" (Array.to_list (Array.map string_of_int outer_chunk_shape)))
        (String.concat ";" (Array.to_list (Array.map string_of_int (Ndarray.shape arr)))));
    (* Stage 1: encode every inner chunk independently (parallel). *)
    let encoded = Array.make num_inner_chunks None in
    Parallel.iter ~domains:config.domains num_inner_chunks (fun i ->
      let chunk = Ndarray.empty dtype inner_chunk_shape in
      Ndarray.blit ~src:arr ~src_offset:(inner_start i) ~dst:chunk ~dst_offset:zeros ~shape:inner_chunk_shape;
      let skip = elide_empty && Ndarray.equals_fill chunk (Option.get fill_value) in
      if not skip then encoded.(i) <- Some (Codec_intf.encode_chain inner_chain chunk));
    (* Stage 2: lay the chunks out in Morton order and build the index. *)
    let data_start = match index_location with IL.Start -> index_size | IL.End -> 0 in
    let index = Array.make num_inner_chunks empty_entry in
    let data_size =
      Array.fold_left (fun pos i ->
        match encoded.(i) with
        | None -> pos
        | Some bytes ->
          let nbytes = Bytes.length bytes in
          index.(i) <- { offset = Int64.of_int (data_start + pos); nbytes = Int64.of_int nbytes };
          pos + nbytes
      ) 0 layout_order
    in
    let encoded_index = encode_index index_chain index in
    let shard = Bytes.create (index_size + data_size) in
    Array.iteri (fun i chunk ->
      Option.iter (fun bytes ->
        Bytes.blit bytes 0 shard (Int64.to_int index.(i).offset) (Bytes.length bytes)) chunk
    ) encoded;
    let index_start = match index_location with IL.Start -> 0 | IL.End -> data_size in
    Bytes.blit encoded_index 0 shard index_start index_size;
    shard
  in

  let decode shape dtype bytes =
    if shape <> outer_chunk_shape then
      raise (Codec_intf.Decode_error "sharding: requested shape differs from the shard shape");
    let shard_size = Bytes.length bytes in
    if shard_size < index_size then
      raise (Codec_intf.Decode_error
        (Printf.sprintf "sharding: shard of %d bytes is smaller than its %d-byte index" shard_size index_size));
    let index_start = match index_location with IL.Start -> 0 | IL.End -> shard_size - index_size in
    let index = decode_index index_chain num_inner_chunks (Bytes.sub bytes index_start index_size) in
    let result =
      match fill_value with
      | Some fv -> Ndarray.make dtype shape fv
      | None -> Ndarray.create dtype shape
    in
    Parallel.iter ~domains:config.domains num_inner_chunks (fun i ->
      let entry = index.(i) in
      if not (is_empty_entry entry) then begin
        let offset = Int64.to_int entry.offset and nbytes = Int64.to_int entry.nbytes in
        if offset < 0 || nbytes < 0 || offset + nbytes > shard_size then
          raise (Codec_intf.Decode_error
            (Printf.sprintf "sharding: inner chunk %d at offset %d (%d bytes) lies outside the shard" i offset nbytes));
        let chunk = Codec_intf.decode_chain inner_chain inner_chunk_shape dtype (Bytes.sub bytes offset nbytes) in
        Ndarray.blit ~src:chunk ~src_offset:zeros ~dst:result ~dst_offset:(inner_start i) ~shape:inner_chunk_shape
      end);
    result
  in
  ({ encode; decode } : Codec_intf.array_to_bytes)
