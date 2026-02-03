(** Tests for Blosc codec *)

open Alcotest
open Zarr_blosc

let test_blosc_roundtrip () =
  let input = Bytes.of_string "Hello, World! This is some test data to compress with blosc." in
  let codec = Blosc.create
    ~cname:Blosc.LZ4
    ~clevel:5
    ~shuffle:Blosc.NoShuffle
    ~typesize:1
    ~blocksize:0 in
  let compressed = codec.encode input in
  check bool "compression works" true (Bytes.length compressed > 0);
  match codec.decode compressed with
  | Ok decompressed -> check bytes "roundtrip" input decompressed
  | Error _ -> fail "decompress failed"

let test_blosc_shuffle () =
  (* Create data with 4-byte integers - shuffle works well with typed data *)
  let n = 1000 in
  let input = Bytes.create (n * 4) in
  for i = 0 to n - 1 do
    Bytes.set_int32_le input (i * 4) (Int32.of_int i)
  done;
  let codec = Blosc.create
    ~cname:Blosc.LZ4
    ~clevel:5
    ~shuffle:Blosc.Shuffle
    ~typesize:4
    ~blocksize:0 in
  let compressed = codec.encode input in
  match codec.decode compressed with
  | Ok decompressed -> check bytes "shuffle roundtrip" input decompressed
  | Error _ -> fail "decompress failed"

let test_blosc_bitshuffle () =
  let n = 1000 in
  let input = Bytes.create (n * 4) in
  for i = 0 to n - 1 do
    Bytes.set_int32_le input (i * 4) (Int32.of_int i)
  done;
  let codec = Blosc.create
    ~cname:Blosc.LZ4
    ~clevel:5
    ~shuffle:Blosc.BitShuffle
    ~typesize:4
    ~blocksize:0 in
  let compressed = codec.encode input in
  match codec.decode compressed with
  | Ok decompressed -> check bytes "bitshuffle roundtrip" input decompressed
  | Error _ -> fail "decompress failed"

let test_blosc_compressors () =
  let input = Bytes.init 10000 (fun i -> Char.chr (i mod 256)) in
  let compressors = [
    ("lz4", Blosc.LZ4);
    ("lz4hc", Blosc.LZ4HC);
    ("blosclz", Blosc.BloscLZ);
    ("zstd", Blosc.Zstd);
    ("zlib", Blosc.Zlib);
  ] in
  List.iter (fun (name, cname) ->
    let codec = Blosc.create
      ~cname
      ~clevel:5
      ~shuffle:Blosc.Shuffle
      ~typesize:1
      ~blocksize:0 in
    let compressed = codec.encode input in
    match codec.decode compressed with
    | Ok decompressed ->
      check bytes (name ^ " roundtrip") input decompressed
    | Error (`Codec_error msg) ->
      fail (name ^ " decompress failed: " ^ msg)
    | Error _ ->
      fail (name ^ " decompress failed")
  ) compressors

let test_blosc_levels () =
  let input = Bytes.of_string (String.make 10000 'a') in
  let codec1 = Blosc.create
    ~cname:Blosc.LZ4
    ~clevel:1
    ~shuffle:Blosc.NoShuffle
    ~typesize:1
    ~blocksize:0 in
  let codec9 = Blosc.create
    ~cname:Blosc.LZ4
    ~clevel:9
    ~shuffle:Blosc.NoShuffle
    ~typesize:1
    ~blocksize:0 in
  let compressed1 = codec1.encode input in
  let compressed9 = codec9.encode input in
  check bool "level 9 <= level 1"
    true (Bytes.length compressed9 <= Bytes.length compressed1)

let test_blosc_large_data () =
  let input = Bytes.init 100000 (fun i -> Char.chr (i mod 256)) in
  let codec = Blosc.create
    ~cname:Blosc.LZ4
    ~clevel:5
    ~shuffle:Blosc.Shuffle
    ~typesize:1
    ~blocksize:0 in
  let compressed = codec.encode input in
  match codec.decode compressed with
  | Ok decompressed -> check bytes "large roundtrip" input decompressed
  | Error _ -> fail "decompress large failed"

let test_blosc_decode_invalid () =
  let input = Bytes.of_string "this is not valid blosc data!!" in
  let codec = Blosc.create
    ~cname:Blosc.LZ4
    ~clevel:5
    ~shuffle:Blosc.NoShuffle
    ~typesize:1
    ~blocksize:0 in
  match codec.decode input with
  | Ok _ -> fail "should fail on invalid data"
  | Error _ -> ()

let test_blosc_shuffle_modes_string () =
  check (result (testable (fun fmt _ -> Format.pp_print_string fmt "<shuffle>") (fun _ _ -> true))
    (testable (fun fmt _ -> Format.pp_print_string fmt "<error>") (fun _ _ -> true)))
    "noshuffle" (Ok Blosc.NoShuffle) (Blosc.shuffle_of_string "noshuffle");
  check (result (testable (fun fmt _ -> Format.pp_print_string fmt "<shuffle>") (fun _ _ -> true))
    (testable (fun fmt _ -> Format.pp_print_string fmt "<error>") (fun _ _ -> true)))
    "shuffle" (Ok Blosc.Shuffle) (Blosc.shuffle_of_string "shuffle");
  check string "noshuffle to_string" "noshuffle" (Blosc.shuffle_to_string Blosc.NoShuffle);
  check string "shuffle to_string" "shuffle" (Blosc.shuffle_to_string Blosc.Shuffle);
  check string "bitshuffle to_string" "bitshuffle" (Blosc.shuffle_to_string Blosc.BitShuffle)

let () =
  run "zarr-blosc" [
    "blosc", [
      "roundtrip", `Quick, test_blosc_roundtrip;
      "shuffle", `Quick, test_blosc_shuffle;
      "bitshuffle", `Quick, test_blosc_bitshuffle;
      "compressors", `Quick, test_blosc_compressors;
      "levels", `Quick, test_blosc_levels;
      "large data", `Quick, test_blosc_large_data;
      "decode invalid", `Quick, test_blosc_decode_invalid;
      "shuffle modes string", `Quick, test_blosc_shuffle_modes_string;
    ]
  ]
