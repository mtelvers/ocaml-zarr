(* Integration tests for the S3 store against a live S3-compatible server.

     S3_ENDPOINT=http://localhost:9000 S3_ACCESS_KEY=... S3_SECRET_KEY=... \
       ./_build/default/test_s3/test_s3.exe

   Optional: S3_BUCKET (default zarr-itest, created if missing), S3_REGION. *)

open Alcotest
module Store = Zarr_s3.Store
module Arr = Zarr_s3.Array
module Group = Zarr_s3.Group

let or_fail label = function
  | Ok v -> v
  | Error e -> failf "%s: %a" label S3.Client.pp_error e

let ramp shape =
  Zarr.Ndarray.init Float32 shape (fun idx ->
    `Float (Float.of_int (Array.fold_left (fun acc i -> acc * 100 + i) 0 idx)))

let tests store =
  let test_store_ops () =
    or_fail "set" (Store.set store "k/a" (Bytes.of_string "hello"));
    or_fail "set" (Store.set store "k/sub/b" (Bytes.of_string "world"));
    check (option bytes) "get" (Some (Bytes.of_string "hello")) (or_fail "get" (Store.get store "k/a"));
    check (option bytes) "get missing" None (or_fail "get" (Store.get store "k/missing"));
    check bool "exists" true (or_fail "exists" (Store.exists store "k/a"));
    check bool "not exists" false (or_fail "exists" (Store.exists store "k/missing"));
    check (option (list bytes)) "get_partial"
      (Some [Bytes.of_string "ell"; Bytes.of_string "lo"])
      (or_fail "get_partial" (Store.get_partial store "k/a" [(1, Some 3); (3, None)]));
    or_fail "set_partial" (Store.set_partial store [("k/a", 5, Bytes.of_string "!!")]);
    check (option bytes) "patched" (Some (Bytes.of_string "hello!!")) (or_fail "get" (Store.get store "k/a"));
    check (pair (list string) (list string)) "list_dir" (["k/a"], ["k/sub/"])
      (or_fail "list_dir" (Store.list_dir store "k/"));
    check (list string) "list_prefix" ["k/sub/b"] (or_fail "list_prefix" (Store.list_prefix store "k/sub"));
    or_fail "erase" (Store.erase store "k/a");
    or_fail "erase missing" (Store.erase store "k/a");
    or_fail "erase_prefix" (Store.erase_prefix store "k/");
    check (list string) "all gone" [] (or_fail "list_prefix" (Store.list_prefix store "k/"))
  in
  let test_sharded_array () =
    let codecs = [ Zarr.sharding_codec ~chunk_shape:[|4; 8|]
                     ~codecs:[Zarr.bytes_codec (); Zarr.zstd_codec ()] () ] in
    let arr = match Arr.create store ~path:"grp/emb" ~shape:[|20; 16|] ~chunks:[|8; 16|]
                      ~dtype:Float32 ~fill_value:(Float 0.0) ~codecs () with
      | Ok a -> a | Error _ -> fail "create array"
    in
    let data = ramp [|20; 16|] in
    (* zero the first inner chunk of shard (0,0) so that it is omitted *)
    Zarr.Ndarray.blit ~src:(Zarr.Ndarray.create Float32 [|4; 8|]) ~src_offset:[|0; 0|]
      ~dst:data ~dst_offset:[|0; 0|] ~shape:[|4; 8|];
    Arr.set_slice arr [Zarr.All; Zarr.All] data;
    let back = Arr.get_slice arr [Zarr.All; Zarr.All] in
    check bool "roundtrip through S3" true (Zarr.Ndarray.equal data back);
    (* the first inner chunk of shard (0,0) is all zero: it is omitted, so the
       shard is smaller than a full one *)
    let s00 = Option.get (or_fail "get" (Store.get store "grp/emb/c/0/0")) in
    let s10 = Option.get (or_fail "get" (Store.get store "grp/emb/c/1/0")) in
    check bool "zero inner chunk omitted" true (Bytes.length s00 < Bytes.length s10);
    (* reopen and read a window *)
    let arr = match Arr.open_ store ~path:"grp/emb" with Ok a -> a | Error _ -> fail "open" in
    let window = Arr.get_slice arr [Zarr.Range (6, 18); Zarr.Range (3, 9)] in
    check (array int) "window shape" [|12; 6|] (Zarr.Ndarray.shape window);
    check bool "window value" true (Zarr.Ndarray.get window [|0; 0|] = Zarr.Ndarray.get data [|6; 3|]);
    (* all-fill write deletes the shard *)
    Arr.set_slice arr [Zarr.Range (16, 20); Zarr.All] (Zarr.Ndarray.create Float32 [|4; 16|]);
    check bool "empty shard deleted" false (or_fail "exists" (Store.exists store "grp/emb/c/2/0"))
  in
  let test_groups () =
    let g = match Group.create store ~path:"grp" () with Ok g -> g | Error _ -> fail "create group" in
    check (list string) "children" ["emb"] (Group.children g);
    check bool "child type" true (Group.child_type g "emb" = Some `Array);
    Zarr_s3.Hierarchy.delete store "/grp";
    check (list string) "deleted" [] (or_fail "list" (Store.list_prefix store "grp"))
  in
  [
    "store operations", `Quick, test_store_ops;
    "sharded array roundtrip", `Quick, test_sharded_array;
    "groups", `Quick, test_groups;
  ]

let () =
  match Sys.getenv_opt "S3_ENDPOINT" with
  | None ->
    print_endline "S3_ENDPOINT not set; skipping S3 integration tests.";
    exit 0
  | Some endpoint ->
    let env_or name default = Option.value ~default (Sys.getenv_opt name) in
    let credentials = { S3.Credentials.access_key = env_or "S3_ACCESS_KEY" "minioadmin";
                        secret_key = env_or "S3_SECRET_KEY" "minioadmin"; session_token = None } in
    let bucket = env_or "S3_BUCKET" "zarr-itest" in
    Eio_main.run @@ fun env ->
    Eio.Switch.run @@ fun sw ->
    let config = S3.Client.make_config ~endpoint ~credentials ?region:(Sys.getenv_opt "S3_REGION") () in
    let client = S3.Client.create ~sw ~net:(Eio.Stdenv.net env) ~clock:(Eio.Stdenv.clock env) config in
    (match S3.Client.bucket_exists client ~bucket with
     | Ok true -> ()
     | Ok false -> or_fail "create bucket" (S3.Client.create_bucket client ~bucket)
     | Error e -> failf "bucket_exists: %a" S3.Client.pp_error e);
    let store = Store.create client ~bucket ~prefix:"zarr-test" in
    or_fail "clean" (Store.erase_prefix store "");
    Alcotest.run ~and_exit:false "zarr-s3" [ ("s3", tests store) ];
    or_fail "clean" (Store.erase_prefix store "")
