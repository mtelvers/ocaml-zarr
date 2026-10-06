(** Benchmarks for Zarr codecs *)

open Zarr

(** Simple timing function *)
let time_it name iterations f =
  let start = Unix.gettimeofday () in
  for _ = 1 to iterations do
    ignore (f ())
  done;
  let elapsed = Unix.gettimeofday () -. start in
  let per_iter = elapsed /. Float.of_int iterations *. 1000.0 in
  Printf.printf "%-40s: %8.3f ms/iter (%d iterations, %.2f s total)\n%!"
    name per_iter iterations elapsed

(** Generate random data *)
let random_bytes n =
  let buf = Bytes.create n in
  for i = 0 to n - 1 do
    Bytes.set buf i (Char.chr (Random.int 256))
  done;
  buf

let random_int32_array shape =
  let arr = Ndarray.create Int32 shape in
  let n = Ndarray.numel arr in
  let dims = Ndarray.shape arr in
  for i = 0 to n - 1 do
    let idx = Ndarray.offset_to_index dims i in
    Ndarray.set arr idx (`Int32 (Random.int32 Int32.max_int))
  done;
  arr

let random_float64_array shape =
  let arr = Ndarray.create Float64 shape in
  let n = Ndarray.numel arr in
  let dims = Ndarray.shape arr in
  for i = 0 to n - 1 do
    let idx = Ndarray.offset_to_index dims i in
    Ndarray.set arr idx (`Float (Random.float 1000.0))
  done;
  arr

(** {2 Codec Benchmarks} *)

let bench_bytes_codec () =
  Printf.printf "\n=== Bytes Codec ===\n";

  let arr_small = random_int32_array [|100; 100|] in
  let arr_medium = random_int32_array [|500; 500|] in
  let arr_large = random_int32_array [|1000; 1000|] in

  let codec = Codecs.Bytes_codec.create Little in

  time_it "bytes encode 100x100 int32" 1000 (fun () -> codec.encode arr_small);
  time_it "bytes encode 500x500 int32" 100 (fun () -> codec.encode arr_medium);
  time_it "bytes encode 1000x1000 int32" 10 (fun () -> codec.encode arr_large);

  let encoded_small = codec.encode arr_small in
  let encoded_medium = codec.encode arr_medium in
  let encoded_large = codec.encode arr_large in

  time_it "bytes decode 100x100 int32" 1000 (fun () ->
    codec.decode [|100; 100|] Int32 encoded_small);
  time_it "bytes decode 500x500 int32" 100 (fun () ->
    codec.decode [|500; 500|] Int32 encoded_medium);
  time_it "bytes decode 1000x1000 int32" 10 (fun () ->
    codec.decode [|1000; 1000|] Int32 encoded_large)

let bench_gzip_codec () =
  Printf.printf "\n=== Gzip Codec ===\n";

  let data_1k = random_bytes 1024 in
  let data_10k = random_bytes 10240 in
  let data_100k = random_bytes 102400 in

  let codec = Codecs.Gzip.create 6 in

  time_it "gzip encode 1KB" 1000 (fun () -> codec.encode data_1k);
  time_it "gzip encode 10KB" 100 (fun () -> codec.encode data_10k);
  time_it "gzip encode 100KB" 10 (fun () -> codec.encode data_100k);

  let compressed_1k = codec.encode data_1k in
  let compressed_10k = codec.encode data_10k in
  let compressed_100k = codec.encode data_100k in

  Printf.printf "  Compression ratios: 1KB=%.2fx, 10KB=%.2fx, 100KB=%.2fx\n"
    (Float.of_int (Bytes.length data_1k) /. Float.of_int (Bytes.length compressed_1k))
    (Float.of_int (Bytes.length data_10k) /. Float.of_int (Bytes.length compressed_10k))
    (Float.of_int (Bytes.length data_100k) /. Float.of_int (Bytes.length compressed_100k));

  time_it "gzip decode 1KB" 1000 (fun () -> ignore (codec.decode compressed_1k));
  time_it "gzip decode 10KB" 100 (fun () -> ignore (codec.decode compressed_10k));
  time_it "gzip decode 100KB" 10 (fun () -> ignore (codec.decode compressed_100k))

let bench_crc32c_codec () =
  Printf.printf "\n=== CRC32C Codec ===\n";

  let data_1k = random_bytes 1024 in
  let data_10k = random_bytes 10240 in
  let data_100k = random_bytes 102400 in

  time_it "crc32c encode 1KB" 10000 (fun () -> Codecs.Crc32c.encode data_1k);
  time_it "crc32c encode 10KB" 1000 (fun () -> Codecs.Crc32c.encode data_10k);
  time_it "crc32c encode 100KB" 100 (fun () -> Codecs.Crc32c.encode data_100k);

  let encoded_1k = Codecs.Crc32c.encode data_1k in
  let encoded_10k = Codecs.Crc32c.encode data_10k in
  let encoded_100k = Codecs.Crc32c.encode data_100k in

  time_it "crc32c decode 1KB" 10000 (fun () -> ignore (Codecs.Crc32c.decode encoded_1k));
  time_it "crc32c decode 10KB" 1000 (fun () -> ignore (Codecs.Crc32c.decode encoded_10k));
  time_it "crc32c decode 100KB" 100 (fun () -> ignore (Codecs.Crc32c.decode encoded_100k))

let bench_transpose_codec () =
  Printf.printf "\n=== Transpose Codec ===\n";

  let arr_2d = random_int32_array [|100; 100|] in
  let arr_3d = random_int32_array [|50; 50; 50|] in

  let codec_2d = Codecs.Transpose.create [|1; 0|] in
  let codec_3d = Codecs.Transpose.create [|2; 0; 1|] in

  time_it "transpose 2D 100x100" 100 (fun () -> codec_2d.encode arr_2d);
  time_it "transpose 3D 50x50x50" 10 (fun () -> codec_3d.encode arr_3d);

  let transposed_2d = codec_2d.encode arr_2d in
  let transposed_3d = codec_3d.encode arr_3d in

  time_it "transpose inverse 2D" 100 (fun () -> codec_2d.decode transposed_2d);
  time_it "transpose inverse 3D" 10 (fun () -> codec_3d.decode transposed_3d)

(** {2 Codec Chain Benchmarks} *)

let bench_codec_chain () =
  Printf.printf "\n=== Codec Chain ===\n";

  let arr = random_float64_array [|100; 100|] in

  (* Bytes only *)
  (match Codec.build_chain [Bytes { endian = Some Little }] Float64 [|100; 100|] with
   | Error _ -> Printf.printf "Failed to build bytes chain\n"
   | Ok chain ->
     time_it "chain: bytes only encode" 1000 (fun () -> Codec.encode chain arr);
     let encoded = Codec.encode chain arr in
     time_it "chain: bytes only decode" 1000 (fun () ->
       ignore (Codec.decode chain [|100; 100|] Float64 encoded)));

  (* Bytes + gzip *)
  (match Codec.build_chain [Bytes { endian = Some Little }; Gzip { level = 5 }] Float64 [|100; 100|] with
   | Error _ -> Printf.printf "Failed to build bytes+gzip chain\n"
   | Ok chain ->
     time_it "chain: bytes+gzip encode" 100 (fun () -> Codec.encode chain arr);
     let encoded = Codec.encode chain arr in
     time_it "chain: bytes+gzip decode" 100 (fun () ->
       ignore (Codec.decode chain [|100; 100|] Float64 encoded)))

(** {2 Array Operation Benchmarks} *)

let bench_array_ops () =
  Printf.printf "\n=== Array Operations ===\n";

  let store = Zarr_sync.Memory_store.create () in

  (* Create array *)
  time_it "create 1000x1000 array" 10 (fun () ->
    match Zarr_sync.Memory_array.create store
      ~path:"bench"
      ~shape:[|1000; 1000|]
      ~chunks:[|100; 100|]
      ~dtype:Float64
      ~fill_value:(Float 0.0)
      () with
    | Ok _ -> Zarr_sync.Memory_store.clear store
    | Error _ -> ());

  (* Set up array for read/write tests *)
  let arr = match Zarr_sync.Memory_array.create store
    ~path:"bench"
    ~shape:[|1000; 1000|]
    ~chunks:[|100; 100|]
    ~dtype:Float64
    ~fill_value:(Float 0.0)
    () with
    | Ok a -> a
    | Error _ -> failwith "Failed to create array"
  in

  let data_100x100 = random_float64_array [|100; 100|] in

  (* Write chunks *)
  time_it "write 100x100 chunk" 100 (fun () ->
    Zarr_sync.Memory_array.set_slice arr [Range (0, 100); Range (0, 100)] data_100x100);

  (* Read chunks *)
  time_it "read 100x100 chunk" 100 (fun () ->
    ignore (Zarr_sync.Memory_array.get_slice arr [Range (0, 100); Range (0, 100)]));

  (* Cross-chunk read *)
  time_it "read 150x150 cross-chunk" 50 (fun () ->
    ignore (Zarr_sync.Memory_array.get_slice arr [Range (50, 200); Range (50, 200)]))

(** {2 Main} *)

let () =
  Printf.printf "Zarr Codec Benchmarks\n";
  Printf.printf "=====================\n";

  Random.init 42;

  bench_bytes_codec ();
  bench_gzip_codec ();
  bench_crc32c_codec ();
  bench_transpose_codec ();
  bench_codec_chain ();
  bench_array_ops ()

module Array = Stdlib.Array  (* [open Zarr] shadows it with the array functor *)

(** {2 Sharding Benchmarks}

    A scaled-down Tessera embeddings shard: int8, inner chunks of
    (1,128,32,32) = 128 KiB, compressed with zstd.  The real shard is
    (1,128,4096,4096) with 16384 inner chunks; this one has 256. *)

let shard_shape = [|1; 128; 512; 512|]
let inner_shape = [|1; 128; 32; 32|]

(* Low-entropy data so the compressor has real work; every third inner chunk
   is left as fill value (zero) to exercise omission. *)
let embeddings_like () =
  let arr = Ndarray.create Int8 shard_shape in
  let n = Ndarray.numel arr in
  (match arr with
   | Ndarray.Int8_signed ga ->
     let flat = Bigarray.reshape_1 ga n in
     let row = shard_shape.(3) in
     for i = 0 to n - 1 do
       let y = (i / row) mod shard_shape.(2) and x = i mod row in
       let chunk_id = (y / 32) * (row / 32) + (x / 32) in
       if chunk_id mod 3 <> 0 then
         Bigarray.Array1.unsafe_set flat i ((((i * 7919) lsr 7) land 0x1f) - 16)
     done
   | _ -> ());
  arr

let bench_sharding () =
  Printf.printf "\n=== Sharding (int8 %s, inner %s, zstd) ===\n"
    (String.concat "x" (Array.to_list (Array.map string_of_int shard_shape)))
    (String.concat "x" (Array.to_list (Array.map string_of_int inner_shape)));
  let arr = embeddings_like () in
  let spec = sharding_codec ~chunk_shape:inner_shape
      ~codecs:[bytes_codec (); zstd_codec ~level:3 ()] () in
  let chains = List.filter_map (fun domains ->
    let config = { Codec.default_config with domains } in
    match Codec.build_chain ~config ~fill_value:(Int 0L) [spec] Int8 shard_shape with
    | Ok chain -> Some (domains, chain)
    | Error _ -> None
  ) [1; min 8 (Domain.recommended_domain_count ())] in
  List.iter (fun (domains, chain) ->
    time_it (Printf.sprintf "shard encode, %d domain(s)" domains) 3 (fun () -> Codec.encode chain arr);
    let encoded = Codec.encode chain arr in
    if domains = 1 then
      Printf.printf "  shard: %d MiB raw -> %d KiB encoded (%d inner chunks, 1/3 omitted)\n"
        (Ndarray.numel arr / 1024 / 1024) (Bytes.length encoded / 1024)
        (Codecs.Sharding.total_inner_chunks shard_shape inner_shape);
    time_it (Printf.sprintf "shard decode, %d domain(s)" domains) 3 (fun () ->
      Codec.decode chain shard_shape Int8 encoded)
  ) chains;
  time_it "to_bytes 32 MiB int8" 10 (fun () -> Ndarray.to_bytes Little arr);
  let chunk = Ndarray.empty Int8 inner_shape in
  time_it "blit one inner chunk out" 1000 (fun () ->
    Ndarray.blit ~src:arr ~src_offset:[|0; 0; 64; 96|] ~dst:chunk ~dst_offset:[|0; 0; 0; 0|] ~shape:inner_shape);
  time_it "equals_fill on a zero inner chunk" 1000 (fun () ->
    Ndarray.blit ~src:arr ~src_offset:[|0; 0; 0; 0|] ~dst:chunk ~dst_offset:[|0; 0; 0; 0|] ~shape:inner_shape;
    Ndarray.equals_fill chunk (Int 0L));
  time_it "equals_fill on a zero 32 MiB shard" 10 (fun () ->
    Ndarray.equals_fill (Ndarray.create Int8 shard_shape) (Int 0L))

let () =
  bench_sharding ();
  Printf.printf "\nDone!\n"
