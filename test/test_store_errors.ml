(** Tests for the result-based store interface and its exception adapter *)

open Alcotest

(* A result-based in-memory store whose operations fail while [failing] is set. *)
module Flaky = struct
  type t = { data : (string, bytes) Hashtbl.t; mutable failing : bool }
  type error = string

  let pp_error = Format.pp_print_string
  let create () = { data = Hashtbl.create 8; failing = false }
  let guard t f = if t.failing then Error "store unavailable" else Ok (f ())

  let get t key = guard t (fun () -> Hashtbl.find_opt t.data key)
  let get_partial t key ranges =
    guard t (fun () -> Option.map (fun b -> Zarr.Store.sub_ranges b ranges) (Hashtbl.find_opt t.data key))
  let exists t key = guard t (fun () -> Hashtbl.mem t.data key)
  let set t key value = guard t (fun () -> Hashtbl.replace t.data key value)
  let set_partial t updates =
    guard t (fun () ->
      List.iter (fun (key, offset, bytes) ->
        let existing = Option.value ~default:Bytes.empty (Hashtbl.find_opt t.data key) in
        Hashtbl.replace t.data key (Zarr.Store.patch existing ~offset bytes)) updates)
  let erase t key = guard t (fun () -> Hashtbl.remove t.data key)
  let keys t = Hashtbl.fold (fun k _ acc -> k :: acc) t.data [] |> List.sort compare
  let erase_prefix t prefix =
    guard t (fun () -> List.iter (Hashtbl.remove t.data) (List.filter (String.starts_with ~prefix) (keys t)))
  let list t = guard t (fun () -> keys t)
  let list_prefix t prefix = guard t (fun () -> List.filter (String.starts_with ~prefix) (keys t))
  let list_dir t prefix =
    guard t (fun () ->
      let n = String.length prefix in
      List.fold_left (fun (ks, ps) k ->
        if not (String.starts_with ~prefix k) then (ks, ps)
        else match String.index_from_opt k n '/' with
          | None -> (k :: ks, ps)
          | Some i -> let p = String.sub k 0 (i + 1) in (ks, if List.mem p ps then ps else p :: ps)
      ) ([], []) (keys t)
      |> fun (ks, ps) -> (List.rev ks, List.rev ps))
end

module Store = Zarr.Store.Raise_errors (Flaky)
module Arr = Zarr.Array.Make (Store)
module Group = Zarr.Group.Make (Store)

let test_adapter_passes_values () =
  let s = Flaky.create () in
  Store.set s "a/b" (Bytes.of_string "x");
  Store.set s "a/c/d" (Bytes.of_string "y");
  check (option bytes) "get" (Some (Bytes.of_string "x")) (Store.get s "a/b");
  check (option bytes) "get missing" None (Store.get s "zzz");
  check bool "exists" true (Store.exists s "a/b");
  check (pair (list string) (list string)) "list_dir" (["a/b"], ["a/c/"]) (Store.list_dir s "a/");
  Store.set_partial s [("a/b", 1, Bytes.of_string "yz")];
  check (option bytes) "patched" (Some (Bytes.of_string "xyz")) (Store.get s "a/b");
  check (option (list bytes)) "partial" (Some [Bytes.of_string "yz"]) (Store.get_partial s "a/b" [(1, None)]);
  Store.erase_prefix s "a/c/";
  check (list string) "erase_prefix" ["a/b"] (Store.list s)

let test_adapter_raises () =
  let s = Flaky.create () in
  s.failing <- true;
  check bool "get raises Store_error" true
    (try ignore (Store.get s "k"); false with Zarr.Store.Store_error "store unavailable" -> true)

let test_array_over_result_store () =
  let s = Flaky.create () in
  match Arr.create s ~path:"arr" ~shape:[|6|] ~chunks:[|4|] ~dtype:Int32 () with
  | Error _ -> fail "should create"
  | Ok arr ->
    Arr.set arr [|5|] (`Int32 42l);
    check bool "readback" true (Arr.get arr [|5|] = `Int32 42l);
    check bool "chunk 0 omitted" false (Store.exists s "arr/c/0");
    (match Group.create s ~path:"" () with
     | Ok g -> check (list string) "children" ["arr"] (Group.children g)
     | Error _ -> fail "should create group");
    s.failing <- true;
    check bool "store failure surfaces as Store_error" true
      (try ignore (Arr.get arr [|5|]); false with Zarr.Store.Store_error _ -> true)

let tests = [
  "adapter passes values", `Quick, test_adapter_passes_values;
  "adapter raises", `Quick, test_adapter_raises;
  "array over result store", `Quick, test_array_over_result_store;
]
