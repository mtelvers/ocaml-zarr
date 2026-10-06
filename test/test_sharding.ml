(** Tests for sharding codec *)

open Alcotest
open Zarr
open Zarr_sync

let test_sharding_empty_marker () =
  check int64 "empty marker is -1" Int64.minus_one Codecs.Sharding.empty_marker

let test_sharding_basic () =
  let store = Memory_store.create () in
  match Memory_array.create store
    ~path:"test"
    ~shape:[|32; 32|]
    ~chunks:[|16; 16|]  (* Shard shape *)
    ~dtype:Int32
    ~fill_value:(Int 0L)
    ~codecs:[
      Sharding {
        chunk_shape = [|4; 4|];  (* Inner chunk shape *)
        codecs = [Bytes { endian = Some Little }];
        index_codecs = [Bytes { endian = Some Little }];
        index_location = End;
      }
    ]
    () with
  | Error e ->
    let msg = match e with `Codec_error s -> s | _ -> "?" in
    fail ("should create sharded array: " ^ msg)
  | Ok arr ->
    (* Write some data *)
    let data = Ndarray.create Int32 [|8; 8|] in
    for i = 0 to 7 do
      for j = 0 to 7 do
        Ndarray.set data [|i; j|] (`Int32 (Int32.of_int (i * 8 + j)))
      done
    done;

    Memory_array.set_slice arr [Range (0, 8); Range (0, 8)] data;

    (* Read back *)
    let read_data = Memory_array.get_slice arr [Range (0, 8); Range (0, 8)] in

    for i = 0 to 7 do
      for j = 0 to 7 do
        match Ndarray.get read_data [|i; j|] with
        | `Int32 v ->
          check int32 (Printf.sprintf "element %d,%d" i j)
            (Int32.of_int (i * 8 + j)) v
        | _ -> fail "expected int32"
      done
    done

let test_sharding_with_crc32c () =
  let store = Memory_store.create () in
  match Memory_array.create store
    ~path:"test"
    ~shape:[|32; 32|]
    ~chunks:[|16; 16|]
    ~dtype:Float64
    ~fill_value:(Float 0.0)
    ~codecs:[
      Sharding {
        chunk_shape = [|4; 4|];
        codecs = [Bytes { endian = Some Little }];
        index_codecs = [Bytes { endian = Some Little }; Crc32c];
        index_location = End;
      }
    ]
    () with
  | Error _ -> fail "should create sharded array with crc32c"
  | Ok arr ->
    let data = Ndarray.create Float64 [|4; 4|] in
    for i = 0 to 3 do
      for j = 0 to 3 do
        Ndarray.set data [|i; j|] (`Float (Float.of_int i +. Float.of_int j *. 0.1))
      done
    done;

    Memory_array.set_slice arr [Range (0, 4); Range (0, 4)] data;

    let read_data = Memory_array.get_slice arr [Range (0, 4); Range (0, 4)] in

    for i = 0 to 3 do
      for j = 0 to 3 do
        match Ndarray.get read_data [|i; j|] with
        | `Float v ->
          check (float 0.001) (Printf.sprintf "element %d,%d" i j)
            (Float.of_int i +. Float.of_int j *. 0.1) v
        | _ -> fail "expected float"
      done
    done

let test_sharding_index_start () =
  let store = Memory_store.create () in
  match Memory_array.create store
    ~path:"test"
    ~shape:[|16; 16|]
    ~chunks:[|16; 16|]
    ~dtype:Int32
    ~fill_value:(Int 0L)
    ~codecs:[
      Sharding {
        chunk_shape = [|4; 4|];
        codecs = [Bytes { endian = Some Little }];
        index_codecs = [Bytes { endian = Some Little }];
        index_location = Start;
      }
    ]
    () with
  | Error _ -> fail "should create sharded array with index at start"
  | Ok arr ->
    let data = Ndarray.create Int32 [|4; 4|] in
    for i = 0 to 3 do
      for j = 0 to 3 do
        Ndarray.set data [|i; j|] (`Int32 (Int32.of_int (i + j * 10)))
      done
    done;

    Memory_array.set_slice arr [Range (0, 4); Range (0, 4)] data;

    let read_data = Memory_array.get_slice arr [Range (0, 4); Range (0, 4)] in

    for i = 0 to 3 do
      for j = 0 to 3 do
        match Ndarray.get read_data [|i; j|] with
        | `Int32 v ->
          check int32 (Printf.sprintf "element %d,%d" i j)
            (Int32.of_int (i + j * 10)) v
        | _ -> fail "expected int32"
      done
    done

let test_sharding_3d () =
  let store = Memory_store.create () in
  match Memory_array.create store
    ~path:"test"
    ~shape:[|16; 16; 16|]
    ~chunks:[|8; 8; 8|]
    ~dtype:Int32
    ~fill_value:(Int 0L)
    ~codecs:[
      Sharding {
        chunk_shape = [|2; 2; 2|];
        codecs = [Bytes { endian = Some Little }];
        index_codecs = [Bytes { endian = Some Little }];
        index_location = End;
      }
    ]
    () with
  | Error _ -> fail "should create 3D sharded array"
  | Ok arr ->
    let data = Ndarray.create Int32 [|4; 4; 4|] in
    for i = 0 to 3 do
      for j = 0 to 3 do
        for k = 0 to 3 do
          Ndarray.set data [|i; j; k|] (`Int32 (Int32.of_int (i * 100 + j * 10 + k)))
        done
      done
    done;

    Memory_array.set_slice arr [Range (0, 4); Range (0, 4); Range (0, 4)] data;

    let read_data = Memory_array.get_slice arr [Range (0, 4); Range (0, 4); Range (0, 4)] in

    for i = 0 to 3 do
      for j = 0 to 3 do
        for k = 0 to 3 do
          match Ndarray.get read_data [|i; j; k|] with
          | `Int32 v ->
            check int32 (Printf.sprintf "element %d,%d,%d" i j k)
              (Int32.of_int (i * 100 + j * 10 + k)) v
          | _ -> fail "expected int32"
        done
      done
    done

let test_sharding_codec_spec_json () =
  let spec = Sharding {
    chunk_shape = [|4; 4|];
    codecs = [Bytes { endian = Some Little }];
    index_codecs = [Bytes { endian = Some Little }; Crc32c];
    index_location = End;
  } in
  let json = Codec.spec_to_json spec in
  let json_str = Yojson.Safe.to_string json in
  check bool "has sharding_indexed" true
    (String.length json_str > 0 &&
     (let open String in
      let rec contains s sub i =
        if i > length s - length sub then false
        else if sub = String.sub s i (length sub) then true
        else contains s sub (i + 1)
      in contains json_str "sharding_indexed" 0))

let tests = [
  "empty marker", `Quick, test_sharding_empty_marker;
  "basic sharding", `Quick, test_sharding_basic;
  "sharding with crc32c", `Quick, test_sharding_with_crc32c;
  "sharding index at start", `Quick, test_sharding_index_start;
  "3D sharding", `Quick, test_sharding_3d;
  "codec spec json", `Quick, test_sharding_codec_spec_json;
]

module Array = Stdlib.Array  (* [open Zarr] shadows it with the array functor *)

(* === Empty-chunk omission, fill values, parallelism, byte identity === *)

let sharded_spec ?(index_codecs = [Bytes { endian = Some Little }; Crc32c]) () =
  Sharding {
    chunk_shape = [|4; 4|];
    codecs = [Bytes { endian = Some Little }];
    index_codecs;
    index_location = End;
  }

(* Parse the trailing [bytes + crc32c] index of a shard with [n] inner chunks. *)
let read_index_at_end shard n =
  let index_size = n * 16 + 4 in
  let index_bytes = Bytes.sub shard (Bytes.length shard - index_size) index_size in
  match Codecs.Crc32c.decode index_bytes with
  | Error _ -> fail "shard index checksum"
  | Ok raw ->
    Stdlib.Array.init n (fun i ->
      { Codecs.Sharding.offset = Bytes.get_int64_le raw (i * 16);
        nbytes = Bytes.get_int64_le raw (i * 16 + 8) })

let create_sharded ?config store ~fill_value =
  match Memory_array.create ?config store ~path:"s" ~shape:[|32; 32|] ~chunks:[|16; 16|]
          ~dtype:Int32 ~fill_value ~codecs:[sharded_spec ()] () with
  | Ok arr -> arr
  | Error _ -> fail "should create sharded array"

let block v =
  let data = Ndarray.create Int32 [|4; 4|] in
  Ndarray.fill data (Int (Int64.of_int v));
  data

let test_empty_inner_chunks_omitted () =
  let store = Memory_store.create () in
  let arr = create_sharded store ~fill_value:(Int 0L) in
  (* Only the top-left inner chunk of shard (0,0) holds data. *)
  Memory_array.set_slice arr [Range (0, 4); Range (0, 4)] (block 5);
  let shard = match Memory_store.get store "s/c/0/0" with Some b -> b | None -> fail "shard missing" in
  let index = read_index_at_end shard 16 in
  let empty = Stdlib.Array.fold_left (fun n e -> if Codecs.Sharding.is_empty_entry e then n + 1 else n) 0 index in
  check int "15 of 16 inner chunks omitted" 15 empty;
  check int "shard is one inner chunk plus index" (4 * 4 * 4 + 16 * 16 + 4) (Bytes.length shard);
  check bool "stored chunk is first" true
    (Int64.equal index.(0).offset 0L && Int64.equal index.(0).nbytes 64L);
  (* An all-fill shard is not stored at all. *)
  Memory_array.set_slice arr [Range (16, 32); Range (16, 32)] (Ndarray.create Int32 [|16; 16|]);
  check bool "all-zero shard not written" false (Memory_store.exists store "s/c/1/1");
  (* Overwriting the data with fill removes the shard. *)
  Memory_array.set_slice arr [Range (0, 4); Range (0, 4)] (block 0);
  check bool "shard removed once empty" false (Memory_store.exists store "s/c/0/0")

let test_write_empty_chunks_keeps_everything () =
  let store = Memory_store.create () in
  let config = { Codec.default_config with write_empty_chunks = true } in
  let arr = create_sharded ~config store ~fill_value:(Int 0L) in
  Memory_array.set_slice arr [Range (0, 4); Range (0, 4)] (block 5);
  let shard = match Memory_store.get store "s/c/0/0" with Some b -> b | None -> fail "shard missing" in
  let index = read_index_at_end shard 16 in
  check bool "no inner chunk omitted" true
    (Stdlib.Array.for_all (fun e -> not (Codecs.Sharding.is_empty_entry e)) index);
  check int "full shard" (16 * 64 + 16 * 16 + 4) (Bytes.length shard);
  Memory_array.set_slice arr [Range (16, 32); Range (16, 32)] (Ndarray.create Int32 [|16; 16|]);
  check bool "all-zero shard written" true (Memory_store.exists store "s/c/1/1")

let test_omitted_chunks_read_as_fill_value () =
  let store = Memory_store.create () in
  let arr = create_sharded store ~fill_value:(Int 7L) in
  Memory_array.set_slice arr [Range (0, 4); Range (0, 4)] (block 5);
  (match Memory_array.get arr [|0; 0|] with
   | `Int32 v -> check int32 "written element" 5l v
   | _ -> fail "expected int32");
  (match Memory_array.get arr [|8; 8|] with
   | `Int32 v -> check int32 "omitted inner chunk reads fill value" 7l v
   | _ -> fail "expected int32");
  (* A block equal to the fill value is omitted even though it was written. *)
  Memory_array.set_slice arr [Range (4, 8); Range (4, 8)] (block 7);
  let shard = match Memory_store.get store "s/c/0/0" with Some b -> b | None -> fail "shard missing" in
  let index = read_index_at_end shard 16 in
  check bool "fill-valued block omitted" true (Codecs.Sharding.is_empty_entry index.(5))

let test_parallel_encoding_is_byte_identical () =
  let sequential = Memory_store.create () and parallel = Memory_store.create () in
  let config = { Codec.default_config with domains = 4 } in
  let a1 = create_sharded sequential ~fill_value:(Int 0L) in
  let a2 = create_sharded ~config parallel ~fill_value:(Int 0L) in
  let data = Ndarray.init Int32 [|32; 32|] (fun idx ->
    `Int32 (Int32.of_int (if (idx.(0) / 4 + idx.(1) / 4) mod 3 = 0 then 0 else idx.(0) * 32 + idx.(1)))) in
  Memory_array.set_slice a1 [All; All] data;
  Memory_array.set_slice a2 [All; All] data;
  List.iter (fun key ->
    check (option bytes) key (Memory_store.get sequential key) (Memory_store.get parallel key)
  ) ["s/c/0/0"; "s/c/0/1"; "s/c/1/0"; "s/c/1/1"];
  check bool "parallel read matches" true
    (Ndarray.equal (Memory_array.get_slice a1 [All; All]) (Memory_array.get_slice a2 [All; All]));
  check bool "roundtrip" true (Ndarray.equal data (Memory_array.get_slice a2 [All; All]))

let test_inner_chunk_must_divide_shard () =
  let store = Memory_store.create () in
  match Memory_array.create store ~path:"bad" ~shape:[|32; 32|] ~chunks:[|16; 16|] ~dtype:Int32
          ~codecs:[Sharding { chunk_shape = [|5; 4|]; codecs = [Bytes { endian = Some Little }];
                              index_codecs = [Bytes { endian = Some Little }]; index_location = End }] () with
  | Error (`Codec_error _) -> ()
  | Error _ -> fail "wrong error"
  | Ok _ -> fail "inner chunk shape 5 must not divide shard shape 16"

let test_reencode_python_shard_byte_identical () =
  let dir = "fixtures/python/sharded_int32" in
  if not (Sys.file_exists dir) then Alcotest.skip ();
  let store = Filesystem_store.create dir in
  match Filesystem_array.open_ store ~path:"" with
  | Error _ -> fail "should open fixture"
  | Ok arr ->
    let original = match Filesystem_store.get store "c/1/2" with Some b -> b | None -> fail "shard missing" in
    let decoded = match Codec.decode arr.codec_chain [|64; 64|] Int32 original with
      | Ok d -> d | Error _ -> fail "decode" in
    check bytes "re-encoded shard equals zarr-python's" original (Codec.encode arr.codec_chain decoded)

let tests = tests @ [
  "empty inner chunks omitted", `Quick, test_empty_inner_chunks_omitted;
  "write_empty_chunks keeps everything", `Quick, test_write_empty_chunks_keeps_everything;
  "omitted chunks read as fill value", `Quick, test_omitted_chunks_read_as_fill_value;
  "parallel encoding byte-identical", `Quick, test_parallel_encoding_is_byte_identical;
  "inner chunk must divide shard", `Quick, test_inner_chunk_must_divide_shard;
  "re-encode python shard byte-identical", `Quick, test_reencode_python_shard_byte_identical;
]
