(** Tests for N-dimensional array operations *)

open Alcotest
open Zarr

let test_create_and_shape () =
  let arr = Ndarray.create Int32 [|10; 20; 30|] in
  check (array int) "shape" [|10; 20; 30|] (Ndarray.shape arr);
  check int "ndim" 3 (Ndarray.ndim arr);
  check int "numel" 6000 (Ndarray.numel arr)

let test_create_various_dtypes () =
  let _ = Ndarray.create Bool [|5|] in
  let _ = Ndarray.create Int8 [|5|] in
  let _ = Ndarray.create Int16 [|5|] in
  let _ = Ndarray.create Int32 [|5|] in
  let _ = Ndarray.create Int64 [|5|] in
  let _ = Ndarray.create Uint8 [|5|] in
  let _ = Ndarray.create Uint16 [|5|] in
  let _ = Ndarray.create Float32 [|5|] in
  let _ = Ndarray.create Float64 [|5|] in
  let _ = Ndarray.create Complex64 [|5|] in
  let _ = Ndarray.create Complex128 [|5|] in
  ()

let test_fill () =
  let arr = Ndarray.create Int32 [|3; 3|] in
  Ndarray.fill arr (Int 42L);
  check int "first element" 42
    (match Ndarray.get arr [|0; 0|] with `Int32 i -> Int32.to_int i | _ -> -1);
  check int "last element" 42
    (match Ndarray.get arr [|2; 2|] with `Int32 i -> Int32.to_int i | _ -> -1)

let test_get_set () =
  let arr = Ndarray.create Int32 [|5; 5|] in
  Ndarray.set arr [|2; 3|] (`Int32 123l);
  check int "get after set" 123
    (match Ndarray.get arr [|2; 3|] with `Int32 i -> Int32.to_int i | _ -> -1);
  check int "other still zero" 0
    (match Ndarray.get arr [|0; 0|] with `Int32 i -> Int32.to_int i | _ -> -1)

let test_to_bytes_from_bytes () =
  let arr = Ndarray.create Int32 [|3|] in
  Ndarray.set arr [|0|] (`Int32 1l);
  Ndarray.set arr [|1|] (`Int32 2l);
  Ndarray.set arr [|2|] (`Int32 256l);

  let bytes_le = Ndarray.to_bytes Little arr in
  check int "bytes length" 12 (Bytes.length bytes_le);
  check bytes "first int32 LE"
    (Bytes.of_string "\x01\x00\x00\x00")
    (Bytes.sub bytes_le 0 4);

  let bytes_be = Ndarray.to_bytes Big arr in
  check bytes "first int32 BE"
    (Bytes.of_string "\x00\x00\x00\x01")
    (Bytes.sub bytes_be 0 4);

  let arr2 = Ndarray.of_bytes Int32 Little [|3|] bytes_le in
  check int "roundtrip first" 1
    (match Ndarray.get arr2 [|0|] with `Int32 i -> Int32.to_int i | _ -> -1);
  check int "roundtrip second" 2
    (match Ndarray.get arr2 [|1|] with `Int32 i -> Int32.to_int i | _ -> -1);
  check int "roundtrip third" 256
    (match Ndarray.get arr2 [|2|] with `Int32 i -> Int32.to_int i | _ -> -1)

let test_float64_bytes () =
  let arr = Ndarray.create Float64 [|2|] in
  Ndarray.set arr [|0|] (`Float 1.5);
  Ndarray.set arr [|1|] (`Float 2.5);

  let bytes = Ndarray.to_bytes Little arr in
  check int "float64 bytes length" 16 (Bytes.length bytes);

  let arr2 = Ndarray.of_bytes Float64 Little [|2|] bytes in
  (match Ndarray.get arr2 [|0|] with
   | `Float f -> check (float 0.001) "first float" 1.5 f
   | _ -> fail "expected float");
  (match Ndarray.get arr2 [|1|] with
   | `Float f -> check (float 0.001) "second float" 2.5 f
   | _ -> fail "expected float")

let test_reshape () =
  let arr = Ndarray.create Int32 [|6|] in
  for i = 0 to 5 do
    Ndarray.set arr [|i|] (`Int32 (Int32.of_int i))
  done;
  let arr2 = Ndarray.reshape arr [|2; 3|] in
  check (array int) "new shape" [|2; 3|] (Ndarray.shape arr2);
  check int "element 0,0" 0
    (match Ndarray.get arr2 [|0; 0|] with `Int32 i -> Int32.to_int i | _ -> -1);
  check int "element 0,2" 2
    (match Ndarray.get arr2 [|0; 2|] with `Int32 i -> Int32.to_int i | _ -> -1);
  check int "element 1,0" 3
    (match Ndarray.get arr2 [|1; 0|] with `Int32 i -> Int32.to_int i | _ -> -1)

let test_transpose () =
  let arr = Ndarray.create Int32 [|2; 3|] in
  Ndarray.set arr [|0; 0|] (`Int32 1l);
  Ndarray.set arr [|0; 1|] (`Int32 2l);
  Ndarray.set arr [|0; 2|] (`Int32 3l);
  Ndarray.set arr [|1; 0|] (`Int32 4l);
  Ndarray.set arr [|1; 1|] (`Int32 5l);
  Ndarray.set arr [|1; 2|] (`Int32 6l);

  let arr2 = Ndarray.transpose arr [|1; 0|] in
  check (array int) "transposed shape" [|3; 2|] (Ndarray.shape arr2);
  check int "element 0,0" 1
    (match Ndarray.get arr2 [|0; 0|] with `Int32 i -> Int32.to_int i | _ -> -1);
  check int "element 0,1" 4
    (match Ndarray.get arr2 [|0; 1|] with `Int32 i -> Int32.to_int i | _ -> -1);
  check int "element 2,0" 3
    (match Ndarray.get arr2 [|2; 0|] with `Int32 i -> Int32.to_int i | _ -> -1)

let test_index_conversions () =
  let dims = [|3; 4; 5|] in
  (* Test index_to_offset and offset_to_index are inverses *)
  let idx = [|1; 2; 3|] in
  let offset = Ndarray.index_to_offset dims idx in
  let idx2 = Ndarray.offset_to_index dims offset in
  check (array int) "roundtrip index" idx idx2;

  (* Test specific offset calculation *)
  let offset = Ndarray.index_to_offset [|10; 10|] [|2; 3|] in
  check int "2D offset" 23 offset  (* 2*10 + 3 = 23 *)

let tests = [
  "create and shape", `Quick, test_create_and_shape;
  "create various dtypes", `Quick, test_create_various_dtypes;
  "fill", `Quick, test_fill;
  "get/set", `Quick, test_get_set;
  "to_bytes/from_bytes", `Quick, test_to_bytes_from_bytes;
  "float64 bytes", `Quick, test_float64_bytes;
  "reshape", `Quick, test_reshape;
  "transpose", `Quick, test_transpose;
  "index conversions", `Quick, test_index_conversions;
]

module Array = Stdlib.Array  (* [open Zarr] shadows it with the array functor *)

(* === Block copies, fill-value tests and serialisation of every dtype === *)

let test_blit () =
  let src = Ndarray.init Int32 [|4; 5|] (fun idx -> `Int32 (Int32.of_int (idx.(0) * 10 + idx.(1)))) in
  let dst = Ndarray.create Int32 [|3; 3|] in
  Ndarray.blit ~src ~src_offset:[|1; 2|] ~dst ~dst_offset:[|1; 0|] ~shape:[|2; 3|];
  check bool "copied block" true
    (Ndarray.get dst [|1; 0|] = `Int32 12l && Ndarray.get dst [|2; 2|] = `Int32 24l);
  check bool "untouched row" true (Ndarray.get dst [|0; 0|] = `Int32 0l);
  check bool "out of bounds raises" true
    (try Ndarray.blit ~src ~src_offset:[|3; 3|] ~dst ~dst_offset:[|0; 0|] ~shape:[|2; 3|]; false
     with Invalid_argument _ -> true);
  (* long rows take the memmove path *)
  let big = Ndarray.init Float64 [|3; 2000|] (fun idx -> `Float (Float.of_int (idx.(0) * 2000 + idx.(1)))) in
  let out = Ndarray.create Float64 [|2; 1000|] in
  Ndarray.blit ~src:big ~src_offset:[|1; 500|] ~dst:out ~dst_offset:[|0; 0|] ~shape:[|2; 1000|];
  check bool "long row copy" true
    (Ndarray.get out [|0; 0|] = `Float 2500.0 && Ndarray.get out [|1; 999|] = `Float 5499.0)

let test_blit_strided () =
  let src = Ndarray.init Int32 [|6; 6|] (fun idx -> `Int32 (Int32.of_int (idx.(0) * 10 + idx.(1)))) in
  let dst = Ndarray.create Int32 [|2; 3|] in
  Ndarray.blit_strided ~src ~src_offset:[|1; 0|] ~src_step:[|3; 2|] ~dst ~dst_offset:[|0; 0|] ~shape:[|2; 3|];
  check bool "gathered" true
    (Ndarray.get dst [|0; 0|] = `Int32 10l && Ndarray.get dst [|0; 2|] = `Int32 14l
     && Ndarray.get dst [|1; 1|] = `Int32 42l);
  let back = Ndarray.create Int32 [|6; 6|] in
  Ndarray.scatter_strided ~src:dst ~src_offset:[|0; 0|] ~dst:back ~dst_offset:[|1; 0|] ~dst_step:[|3; 2|] ~shape:[|2; 3|];
  check bool "scattered" true (Ndarray.get back [|4; 4|] = `Int32 44l && Ndarray.get back [|4; 3|] = `Int32 0l)

let test_equals_fill () =
  let nans = Ndarray.make Float32 [|3; 3|] NaN in
  check bool "all NaN equals NaN fill" true (Ndarray.equals_fill nans NaN);
  Ndarray.set nans [|2; 2|] (`Float 1.0);
  check bool "one non-NaN" false (Ndarray.equals_fill nans NaN);
  let ints = Ndarray.make Int8 [|4|] (Int (-3L)) in
  check bool "int8 fill" true (Ndarray.equals_fill ints (Int (-3L)));
  check bool "int8 other value" false (Ndarray.equals_fill ints (Int 3L));
  check bool "wrong kind never matches" false (Ndarray.equals_fill ints (Float 0.0));
  let u = Ndarray.make Uint64 [|2|] (Uint 9L) in
  check bool "uint64 fill" true (Ndarray.equals_fill u (Uint 9L));
  let c = Ndarray.make Complex128 [|2|] (Complex (1.0, -1.0)) in
  check bool "complex fill" true (Ndarray.equals_fill c (Complex (1.0, -1.0)));
  check bool "empty array is all fill" true (Ndarray.equals_fill (Ndarray.create Int32 [|0|]) (Int 0L))

let test_bytes_roundtrip_all_dtypes () =
  let dtypes = Ztypes.Dtype.[Bool; Int8; Int16; Int32; Int64; Uint8; Uint16; Uint32; Uint64;
                            Float16; Float32; Float64; Complex64; Complex128; Raw 8] in
  List.iter (fun dtype ->
    let arr = Ndarray.init dtype [|3; 5|] (fun idx ->
      let k = idx.(0) * 5 + idx.(1) in
      match Ndarray.empty dtype [|1|] with
      | Int8_signed _ -> `Int (k - 7)
      | Int8_unsigned _ | Int16_unsigned _ -> `Int (k * 7)
      | Int16_signed _ -> `Int (k * 100 - 700)
      | Int32 _ -> `Int32 (Int32.of_int (k * 100000 - 1))
      | Int64 _ -> `Int64 (Int64.of_int (k * 1000000007 - 5))
      | Float32 _ -> `Float (Float.of_int k *. 0.5)
      | Float64 _ -> `Float (Float.of_int k *. 1.25 -. 3.0)
      | Complex32 _ | Complex64 _ -> `Complex { Complex.re = Float.of_int k; im = -. Float.of_int k }
      | Char _ -> `Char (Char.chr (k + 65))) in
    List.iter (fun endian ->
      let bytes = Ndarray.to_bytes endian arr in
      check int (Data_type.to_string dtype ^ " byte length")
        (15 * Data_type.size (Ndarray.dtype arr)) (Bytes.length bytes);
      let back = Ndarray.of_bytes dtype endian [|3; 5|] bytes in
      check bool (Data_type.to_string dtype ^ " roundtrip") true (Ndarray.equal arr back)
    ) [Little; Big]
  ) dtypes;
  check bool "short buffer raises" true
    (try ignore (Ndarray.of_bytes Int32 Little [|4|] (Bytes.create 15)); false
     with Invalid_argument _ -> true)

let test_transpose_3d () =
  let arr = Ndarray.init Int32 [|2; 3; 4|] (fun idx -> `Int32 (Int32.of_int (idx.(0) * 100 + idx.(1) * 10 + idx.(2)))) in
  let t = Ndarray.transpose arr [|2; 0; 1|] in
  check (array int) "shape" [|4; 2; 3|] (Ndarray.shape t);
  for i = 0 to 1 do for j = 0 to 2 do for k = 0 to 3 do
    check bool "element" true (Ndarray.get t [|k; i; j|] = Ndarray.get arr [|i; j; k|])
  done done done;
  check bool "inverse" true (Ndarray.equal arr (Ndarray.transpose t [|1; 2; 0|]))

let tests = tests @ [
  "blit", `Quick, test_blit;
  "blit strided", `Quick, test_blit_strided;
  "equals_fill", `Quick, test_equals_fill;
  "bytes roundtrip all dtypes", `Quick, test_bytes_roundtrip_all_dtypes;
  "transpose 3d", `Quick, test_transpose_3d;
]
