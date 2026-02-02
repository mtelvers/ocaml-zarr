(** Tests for codecs *)

open Alcotest
open Zarr

(* Module aliases for nested types *)
module D = Zarr.Ztypes.Dtype
module E = Zarr.Ztypes.Endianness

(* === Bytes codec tests === *)

let test_bytes_little_endian_int32 () =
  let arr = Ndarray.create D.Int32 [|3|] in
  Ndarray.set arr [|0|] (`Int32 1l);
  Ndarray.set arr [|1|] (`Int32 2l);
  Ndarray.set arr [|2|] (`Int32 256l);

  let codec = Codecs.Bytes_codec.create E.Little in
  let encoded = codec.encode arr in

  check bytes "encoded bytes"
    (Bytes.of_string "\x01\x00\x00\x00\x02\x00\x00\x00\x00\x01\x00\x00")
    encoded

let test_bytes_big_endian_int32 () =
  let arr = Ndarray.create D.Int32 [|3|] in
  Ndarray.set arr [|0|] (`Int32 1l);
  Ndarray.set arr [|1|] (`Int32 2l);
  Ndarray.set arr [|2|] (`Int32 256l);

  let codec = Codecs.Bytes_codec.create E.Big in
  let encoded = codec.encode arr in

  check bytes "encoded bytes"
    (Bytes.of_string "\x00\x00\x00\x01\x00\x00\x00\x02\x00\x00\x01\x00")
    encoded

let test_bytes_roundtrip () =
  let arr = Ndarray.create D.Float64 [|5|] in
  for i = 0 to 4 do
    Ndarray.set arr [|i|] (`Float (Float.of_int i *. 1.5))
  done;

  let codec = Codecs.Bytes_codec.create E.Little in
  let encoded = codec.encode arr in
  let decoded = codec.decode [|5|] D.Float64 encoded in

  for i = 0 to 4 do
    match Ndarray.get decoded [|i|] with
    | `Float f -> check (float 0.001) (Printf.sprintf "element %d" i) (Float.of_int i *. 1.5) f
    | _ -> fail "expected float"
  done

(* === CRC32C codec tests === *)

let test_crc32c_known_value () =
  (* iSCSI CRC32C test vector from RFC 3720 *)
  let input = Bytes.of_string "123456789" in
  let with_checksum = Codecs.Crc32c.encode input in
  check int "length increased by 4" (Bytes.length input + 4) (Bytes.length with_checksum);
  (* Verify checksum *)
  let checksum = Bytes.get_int32_le with_checksum (Bytes.length input) in
  check int32 "CRC32C of '123456789'" 0xe3069283l checksum

let test_crc32c_roundtrip () =
  let input = Bytes.of_string "Hello, World!" in
  let encoded = Codecs.Crc32c.encode input in
  match Codecs.Crc32c.decode encoded with
  | Ok decoded -> check bytes "roundtrip" input decoded
  | Error _ -> fail "decode failed"

let test_crc32c_corruption () =
  let input = Bytes.of_string "Hello, World!" in
  let encoded = Codecs.Crc32c.encode input in
  (* Corrupt one byte *)
  Bytes.set encoded 5 'X';
  match Codecs.Crc32c.decode encoded with
  | Ok _ -> fail "should detect corruption"
  | Error `Checksum_mismatch -> ()
  | Error _ -> fail "wrong error type"

(* === Gzip codec tests === *)

let test_gzip_roundtrip () =
  let input = Bytes.of_string "Hello, World! This is some test data to compress." in
  let codec = Codecs.Gzip.create 6 in
  let compressed = codec.encode input in
  (* Compressed should generally be smaller or at least not much larger *)
  check bool "compression works" true (Bytes.length compressed > 0);
  match codec.decode compressed with
  | Ok decompressed -> check bytes "roundtrip" input decompressed
  | Error _ -> fail "decompress failed"

let test_gzip_empty () =
  let input = Bytes.empty in
  let codec = Codecs.Gzip.create 6 in
  let compressed = codec.encode input in
  match codec.decode compressed with
  | Ok decompressed -> check bytes "empty roundtrip" input decompressed
  | Error _ -> fail "decompress empty failed"

let test_gzip_levels () =
  let input = Bytes.of_string (String.make 1000 'a') in
  let codec1 = Codecs.Gzip.create 1 in
  let codec9 = Codecs.Gzip.create 9 in
  let compressed1 = codec1.encode input in
  let compressed9 = codec9.encode input in
  (* Higher compression level should generally produce smaller output for compressible data *)
  check bool "level 9 <= level 1"
    true (Bytes.length compressed9 <= Bytes.length compressed1)

(* === Transpose codec tests === *)

let test_transpose_2d () =
  let arr = Ndarray.create D.Int32 [|2; 3|] in
  (* Set values: [[1,2,3], [4,5,6]] *)
  Ndarray.set arr [|0; 0|] (`Int32 1l);
  Ndarray.set arr [|0; 1|] (`Int32 2l);
  Ndarray.set arr [|0; 2|] (`Int32 3l);
  Ndarray.set arr [|1; 0|] (`Int32 4l);
  Ndarray.set arr [|1; 1|] (`Int32 5l);
  Ndarray.set arr [|1; 2|] (`Int32 6l);

  let codec = Codecs.Transpose.create [|1; 0|] in
  let transposed = codec.encode arr in

  check (array int) "transposed shape" [|3; 2|] (Ndarray.shape transposed);
  (* Check transposed values *)
  check int "element 0,0" 1
    (match Ndarray.get transposed [|0; 0|] with `Int32 i -> Int32.to_int i | _ -> -1);
  check int "element 0,1" 4
    (match Ndarray.get transposed [|0; 1|] with `Int32 i -> Int32.to_int i | _ -> -1);
  check int "element 2,1" 6
    (match Ndarray.get transposed [|2; 1|] with `Int32 i -> Int32.to_int i | _ -> -1)

let test_transpose_roundtrip () =
  let arr = Ndarray.create D.Int32 [|2; 3; 4|] in
  for i = 0 to 1 do
    for j = 0 to 2 do
      for k = 0 to 3 do
        Ndarray.set arr [|i; j; k|] (`Int32 (Int32.of_int (i * 12 + j * 4 + k)))
      done
    done
  done;

  let codec = Codecs.Transpose.create [|2; 0; 1|] in
  let encoded = codec.encode arr in
  let decoded = codec.decode encoded in

  check (array int) "roundtrip shape" [|2; 3; 4|] (Ndarray.shape decoded);
  for i = 0 to 1 do
    for j = 0 to 2 do
      for k = 0 to 3 do
        let expected = i * 12 + j * 4 + k in
        match Ndarray.get decoded [|i; j; k|] with
        | `Int32 v -> check int (Printf.sprintf "element %d,%d,%d" i j k) expected (Int32.to_int v)
        | _ -> fail "expected int32"
      done
    done
  done

(* === Codec chain tests === *)

let test_codec_chain_bytes_only () =
  match Codec.build_chain [Zarr.Bytes { endian = Some E.Little }] D.Int32 [|10|] with
  | Error _ -> fail "should build"
  | Ok chain ->
    let arr = Ndarray.create D.Int32 [|3|] in
    Ndarray.set arr [|0|] (`Int32 1l);
    Ndarray.set arr [|1|] (`Int32 2l);
    Ndarray.set arr [|2|] (`Int32 3l);

    let encoded = Codec.encode chain arr in
    let decoded = Codec.decode chain [|3|] D.Int32 encoded in

    check int "first" 1
      (match Ndarray.get decoded [|0|] with `Int32 i -> Int32.to_int i | _ -> -1);
    check int "second" 2
      (match Ndarray.get decoded [|1|] with `Int32 i -> Int32.to_int i | _ -> -1);
    check int "third" 3
      (match Ndarray.get decoded [|2|] with `Int32 i -> Int32.to_int i | _ -> -1)

let test_codec_chain_with_gzip () =
  match Codec.build_chain [Zarr.Bytes { endian = Some E.Little }; Zarr.Gzip { level = 5 }] D.Int32 [|10|] with
  | Error _ -> fail "should build"
  | Ok chain ->
    let arr = Ndarray.create D.Int32 [|100|] in
    for i = 0 to 99 do
      Ndarray.set arr [|i|] (`Int32 (Int32.of_int i))
    done;

    let encoded = Codec.encode chain arr in
    let decoded = Codec.decode chain [|100|] D.Int32 encoded in

    for i = 0 to 99 do
      match Ndarray.get decoded [|i|] with
      | `Int32 v -> check int (Printf.sprintf "element %d" i) i (Int32.to_int v)
      | _ -> fail "expected int32"
    done

let test_codec_chain_no_array_to_bytes () =
  match Codec.build_chain [Zarr.Gzip { level = 5 }] D.Int32 [|10|] with
  | Error (`Codec_error _) -> ()
  | Ok _ -> fail "should fail without array->bytes"
  | Error _ -> fail "wrong error type"

let tests = [
  "bytes little endian int32", `Quick, test_bytes_little_endian_int32;
  "bytes big endian int32", `Quick, test_bytes_big_endian_int32;
  "bytes roundtrip", `Quick, test_bytes_roundtrip;
  "crc32c known value", `Quick, test_crc32c_known_value;
  "crc32c roundtrip", `Quick, test_crc32c_roundtrip;
  "crc32c corruption", `Quick, test_crc32c_corruption;
  "gzip roundtrip", `Quick, test_gzip_roundtrip;
  "gzip empty", `Quick, test_gzip_empty;
  "gzip levels", `Quick, test_gzip_levels;
  "transpose 2d", `Quick, test_transpose_2d;
  "transpose roundtrip", `Quick, test_transpose_roundtrip;
  "codec chain bytes only", `Quick, test_codec_chain_bytes_only;
  "codec chain with gzip", `Quick, test_codec_chain_with_gzip;
  "codec chain no array->bytes", `Quick, test_codec_chain_no_array_to_bytes;
]
