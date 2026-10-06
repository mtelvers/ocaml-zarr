(** Zarr on S3.

    {!Store} implements {!Zarr.Store.STORE_WITH_ERRORS} over [S3.Client];
    {!Array}, {!Group} and {!Hierarchy} are the Zarr operations instantiated
    on it through {!Zarr.Store.Raise_errors}, so an S3 failure surfaces as
    {!Zarr.Store.Store_error}.

    {[
      Eio_main.run @@ fun env ->
      Eio.Switch.run @@ fun sw ->
      let client =
        S3.Client.create ~sw ~net:(Eio.Stdenv.net env) ~clock:(Eio.Stdenv.clock env)
          (S3.Client.make_config ~endpoint ~credentials ())
      in
      let store = Zarr_s3.Store.create client ~bucket:"tessera" ~prefix:"zarr/v2" in
      match Zarr_s3.Array.open_ store ~path:"embeddings" with
      | Ok arr -> Zarr_s3.Array.get_slice arr [ Zarr.idx 0; Zarr.all; Zarr.range 0 4096; Zarr.range 0 4096 ]
      | Error _ -> ...
    ]} *)

module Store = S3_store

(** The store with S3 errors raised as {!Zarr.Store.Store_error}. *)
module Exn_store = Zarr.Store.Raise_errors (S3_store)

module Array = Zarr.Array.Make (Exn_store)
module Group = Zarr.Group.Make (Exn_store)
module Hierarchy = Zarr.Group.Hierarchy.Make (Exn_store)
