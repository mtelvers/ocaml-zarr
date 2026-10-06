(** Eio-based store implementations for Zarr, with array, group and
    hierarchy operations instantiated for each. *)

module Memory_store = Memory_store
module Filesystem_store = Filesystem_store

module Memory_array = Zarr.Array.Make (Memory_store)
module Filesystem_array = Zarr.Array.Make (Filesystem_store)

module Memory_group = Zarr.Group.Make (Memory_store)
module Filesystem_group = Zarr.Group.Make (Filesystem_store)

module Memory_hierarchy = Zarr.Group.Hierarchy.Make (Memory_store)
module Filesystem_hierarchy = Zarr.Group.Hierarchy.Make (Filesystem_store)

(** Run [f ~fs] inside an Eio main loop. *)
let run f =
  Eio_main.run @@ fun env -> f ~fs:(Eio.Stdenv.fs env)
