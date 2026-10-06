(** Tests for array operations *)

open Alcotest
open Zarr
open Zarr_sync

(* Module aliases for nested types *)
module D = Zarr.Ztypes.Dtype
module E = Zarr.Ztypes.Endianness
module FV = Zarr.Ztypes.Fill_value

let test_create_array () =
  let store = Memory_store.create () in
  match Memory_array.create store
    ~path:"test"
    ~shape:[|100; 100|]
    ~chunks:[|10; 10|]
    ~dtype:D.Float64
    ~fill_value:(FV.Float 0.0)
    ~codecs:[Zarr.Bytes { endian = Some E.Little }]
    () with
  | Error _ -> fail "should create array"
  | Ok arr ->
    check (array int) "shape" [|100; 100|] (Memory_array.shape arr);
    check (array int) "chunks" [|10; 10|] (Memory_array.chunks arr);
    (* Check metadata was written *)
    check bool "metadata exists"
      true (Memory_store.exists store "test/zarr.json")

let test_open_array () =
  let store = Memory_store.create () in
  (match Memory_array.create store
    ~path:"test"
    ~shape:[|50; 50|]
    ~chunks:[|10; 10|]
    ~dtype:D.Int32
    () with
  | Error _ -> fail "should create array"
  | Ok _ -> ());

  match Memory_array.open_ store ~path:"test" with
  | Error _ -> fail "should open array"
  | Ok arr ->
    check (array int) "shape" [|50; 50|] (Memory_array.shape arr)

let test_get_set_scalar () =
  let store = Memory_store.create () in
  match Memory_array.create store
    ~path:"test"
    ~shape:[|10; 10|]
    ~chunks:[|5; 5|]
    ~dtype:D.Int32
    ~fill_value:(FV.Int 0L)
    () with
  | Error _ -> fail "should create array"
  | Ok arr ->
    Memory_array.set arr [|3; 4|] (`Int32 42l);
    match Memory_array.get arr [|3; 4|] with
    | `Int32 v -> check int32 "get after set" 42l v
    | _ -> fail "expected int32"

let test_fill_value () =
  let store = Memory_store.create () in
  match Memory_array.create store
    ~path:"test"
    ~shape:[|10; 10|]
    ~chunks:[|5; 5|]
    ~dtype:D.Float64
    ~fill_value:FV.NaN
    () with
  | Error _ -> fail "should create array"
  | Ok arr ->
    (* Unwritten chunk should return fill value *)
    match Memory_array.get arr [|0; 0|] with
    | `Float f -> check bool "is nan" true (Float.is_nan f)
    | _ -> fail "expected float"

let test_get_set_slice () =
  let store = Memory_store.create () in
  match Memory_array.create store
    ~path:"test"
    ~shape:[|20; 20|]
    ~chunks:[|5; 5|]
    ~dtype:D.Int32
    ~fill_value:(FV.Int 0L)
    () with
  | Error _ -> fail "should create array"
  | Ok arr ->
    (* Create a 5x5 array to write *)
    let data = Ndarray.create D.Int32 [|5; 5|] in
    for i = 0 to 4 do
      for j = 0 to 4 do
        Ndarray.set data [|i; j|] (`Int32 (Int32.of_int (i * 5 + j)))
      done
    done;

    Memory_array.set_slice arr [Zarr.Range (0, 5); Zarr.Range (0, 5)] data;

    (* Read back *)
    let read_data = Memory_array.get_slice arr [Zarr.Range (0, 5); Zarr.Range (0, 5)] in
    check (array int) "shape" [|5; 5|] (Ndarray.shape read_data);

    for i = 0 to 4 do
      for j = 0 to 4 do
        match Ndarray.get read_data [|i; j|] with
        | `Int32 v ->
          check int32 (Printf.sprintf "element %d,%d" i j)
            (Int32.of_int (i * 5 + j)) v
        | _ -> fail "expected int32"
      done
    done

let test_cross_chunk_slice () =
  let store = Memory_store.create () in
  match Memory_array.create store
    ~path:"test"
    ~shape:[|20; 20|]
    ~chunks:[|5; 5|]
    ~dtype:D.Int32
    ~fill_value:(FV.Int 0L)
    () with
  | Error _ -> fail "should create array"
  | Ok arr ->
    (* Write across chunk boundaries *)
    let data = Ndarray.create D.Int32 [|8; 8|] in
    for i = 0 to 7 do
      for j = 0 to 7 do
        Ndarray.set data [|i; j|] (`Int32 (Int32.of_int (i * 8 + j + 100)))
      done
    done;

    Memory_array.set_slice arr [Zarr.Range (3, 11); Zarr.Range (3, 11)] data;

    (* Read back *)
    let read_data = Memory_array.get_slice arr [Zarr.Range (3, 11); Zarr.Range (3, 11)] in
    for i = 0 to 7 do
      for j = 0 to 7 do
        match Ndarray.get read_data [|i; j|] with
        | `Int32 v ->
          check int32 (Printf.sprintf "element %d,%d" i j)
            (Int32.of_int (i * 8 + j + 100)) v
        | _ -> fail "expected int32"
      done
    done

let test_array_with_gzip () =
  let store = Memory_store.create () in
  match Memory_array.create store
    ~path:"test"
    ~shape:[|100; 100|]
    ~chunks:[|10; 10|]
    ~dtype:D.Float64
    ~codecs:[Zarr.Bytes { endian = Some E.Little }; Zarr.Gzip { level = 5 }]
    () with
  | Error _ -> fail "should create array"
  | Ok arr ->
    (* Write some data *)
    let data = Ndarray.create D.Float64 [|10; 10|] in
    for i = 0 to 9 do
      for j = 0 to 9 do
        Ndarray.set data [|i; j|] (`Float (Float.of_int (i * 10 + j)))
      done
    done;

    Memory_array.set_slice arr [Zarr.Range (0, 10); Zarr.Range (0, 10)] data;

    (* Read back *)
    let read_data = Memory_array.get_slice arr [Zarr.Range (0, 10); Zarr.Range (0, 10)] in
    for i = 0 to 9 do
      for j = 0 to 9 do
        match Ndarray.get read_data [|i; j|] with
        | `Float v ->
          check (float 0.001) (Printf.sprintf "element %d,%d" i j)
            (Float.of_int (i * 10 + j)) v
        | _ -> fail "expected float"
      done
    done

let test_array_attributes () =
  let store = Memory_store.create () in
  match Memory_array.create store
    ~path:"test"
    ~shape:[|10|]
    ~chunks:[|10|]
    ~dtype:D.Int32
    () with
  | Error _ -> fail "should create array"
  | Ok arr ->
    Memory_array.set_attrs arr (`Assoc [("key", `String "value")]);
    (* Reopen and check *)
    match Memory_array.open_ store ~path:"test" with
    | Error _ -> fail "should open array"
    | Ok arr2 ->
      let attrs = Memory_array.attrs arr2 in
      match attrs with
      | `Assoc [("key", `String "value")] -> ()
      | _ -> fail "wrong attributes"

let tests = [
  "create array", `Quick, test_create_array;
  "open array", `Quick, test_open_array;
  "get/set scalar", `Quick, test_get_set_scalar;
  "fill value", `Quick, test_fill_value;
  "get/set slice", `Quick, test_get_set_slice;
  "cross chunk slice", `Quick, test_cross_chunk_slice;
  "array with gzip", `Quick, test_array_with_gzip;
  "array attributes", `Quick, test_array_attributes;
]

module Array = Stdlib.Array  (* [open Zarr] shadows it with the array functor *)

(* === Edge chunks, empty chunks, stepped and partial slices === *)

let ramp shape =
  Ndarray.init D.Int32 shape (fun idx ->
    `Int32 (Int32.of_int (Stdlib.Array.fold_left (fun acc i -> acc * 100 + i) 0 idx + 1)))

let test_edge_chunks_stored_full_size () =
  let store = Memory_store.create () in
  match Memory_array.create store ~path:"e" ~shape:[|10; 7|] ~chunks:[|4; 4|] ~dtype:D.Int32 () with
  | Error _ -> fail "should create array"
  | Ok arr ->
    let data = ramp [|10; 7|] in
    Memory_array.set_slice arr [Zarr.All; Zarr.All] data;
    check bool "roundtrip" true (Ndarray.equal data (Memory_array.get_slice arr [Zarr.All; Zarr.All]));
    (* The corner chunk covers only 2x3 elements but is stored at the full 4x4. *)
    (match Memory_store.get store "e/c/2/1" with
     | Some b -> check int "edge chunk is full size" (4 * 4 * 4) (Bytes.length b)
     | None -> fail "edge chunk missing");
    (match Memory_array.get arr [|9; 6|] with
     | `Int32 v -> check int32 "last element" 907l v
     | _ -> fail "expected int32")

let test_all_fill_chunk_not_stored () =
  let store = Memory_store.create () in
  match Memory_array.create store ~path:"z" ~shape:[|8; 8|] ~chunks:[|4; 4|] ~dtype:D.Float64
          ~fill_value:FV.NaN () with
  | Error _ -> fail "should create array"
  | Ok arr ->
    let nans = Ndarray.make D.Float64 [|8; 8|] FV.NaN in
    Ndarray.set nans [|1; 1|] (`Float 2.5);
    Memory_array.set_slice arr [Zarr.All; Zarr.All] nans;
    check bool "chunk with data stored" true (Memory_store.exists store "z/c/0/0");
    check bool "all-NaN chunk not stored" false (Memory_store.exists store "z/c/1/1");
    Memory_array.set arr [|1; 1|] (`Float Float.nan);
    check bool "chunk removed when it becomes all fill" false (Memory_store.exists store "z/c/0/0");
    let config = { Zarr.Codec.default_config with write_empty_chunks = true } in
    (match Memory_array.open_ ~config store ~path:"z" with
     | Error _ -> fail "should reopen"
     | Ok arr ->
       Memory_array.set_slice arr [Zarr.Range (4, 8); Zarr.Range (4, 8)] (Ndarray.make D.Float64 [|4; 4|] FV.NaN);
       check bool "all-fill chunk stored when asked" true (Memory_store.exists store "z/c/1/1"))

let test_stepped_slices () =
  let store = Memory_store.create () in
  match Memory_array.create store ~path:"s" ~shape:[|10; 10|] ~chunks:[|3; 4|] ~dtype:D.Int32 () with
  | Error _ -> fail "should create array"
  | Ok arr ->
    let data = ramp [|10; 10|] in
    Memory_array.set_slice arr [Zarr.All; Zarr.All] data;
    let sub = Memory_array.get_slice arr [Zarr.Stepped (1, 10, 3); Zarr.Stepped (0, 10, 4)] in
    check (array int) "stepped shape" [|3; 3|] (Ndarray.shape sub);
    for i = 0 to 2 do
      for j = 0 to 2 do
        check bool (Printf.sprintf "stepped element %d,%d" i j) true
          (Ndarray.get sub [|i; j|] = Ndarray.get data [|1 + 3 * i; 4 * j|])
      done
    done;
    (* Writing through a stepped slice only touches the selected elements. *)
    let patch = Ndarray.make D.Int32 [|3; 3|] (FV.Int (-1L)) in
    Memory_array.set_slice arr [Zarr.Stepped (1, 10, 3); Zarr.Stepped (0, 10, 4)] patch;
    let all = Memory_array.get_slice arr [Zarr.All; Zarr.All] in
    for i = 0 to 9 do
      for j = 0 to 9 do
        let selected = (i - 1) >= 0 && (i - 1) mod 3 = 0 && j mod 4 = 0 in
        let expected = if selected then `Int32 (-1l) else Ndarray.get data [|i; j|] in
        check bool (Printf.sprintf "after stepped write %d,%d" i j) true (Ndarray.get all [|i; j|] = expected)
      done
    done

let test_partial_slice_list_and_index () =
  let store = Memory_store.create () in
  match Memory_array.create store ~path:"p" ~shape:[|4; 6; 5|] ~chunks:[|2; 2; 2|] ~dtype:D.Int32 () with
  | Error _ -> fail "should create array"
  | Ok arr ->
    let data = ramp [|4; 6; 5|] in
    Memory_array.set_slice arr [] data;
    let row = Memory_array.get_slice arr [Zarr.Index 2] in
    check (array int) "index keeps a unit dimension" [|1; 6; 5|] (Ndarray.shape row);
    check bool "index values" true (Ndarray.get row [|0; 3; 4|] = Ndarray.get data [|2; 3; 4|]);
    let tail = Memory_array.get_slice arr [Zarr.RangeFrom 3; Zarr.RangeTo (-2)] in
    check (array int) "missing trailing slices select everything" [|1; 4; 5|] (Ndarray.shape tail);
    check bool "invalid slice raises" true
      (try ignore (Memory_array.get_slice arr [Zarr.Range (3, 3)]); false with Invalid_argument _ -> true);
    check bool "shape mismatch raises" true
      (try Memory_array.set_slice arr [Zarr.Index 0] (Ndarray.create D.Int32 [|6; 5|]); false
       with Invalid_argument _ -> true);
    Memory_array.delete arr;
    check int "delete removes metadata and chunks" 0 (Memory_store.length store)

let tests = tests @ [
  "edge chunks stored full size", `Quick, test_edge_chunks_stored_full_size;
  "all-fill chunk not stored", `Quick, test_all_fill_chunk_not_stored;
  "stepped slices", `Quick, test_stepped_slices;
  "partial slice list and index", `Quick, test_partial_slice_list_and_index;
]
