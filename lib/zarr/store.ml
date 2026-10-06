(** Store interfaces for Zarr v3.

    A store maps string keys to byte values.  {!STORE} is the exception-based
    interface the array and group functors consume; {!STORE_WITH_ERRORS} is the
    result-based interface that remote stores implement, and {!Raise_errors}
    turns the latter into the former. *)

(** Readable store operations *)
module type READABLE = sig
  type t

  (** Get the full contents of a key, or [None] if it does not exist. *)
  val get : t -> string -> bytes option

  (** Get byte ranges of a key.  Each range is [(offset, length)]; a [None]
      length runs to the end of the value.  [None] if the key does not exist. *)
  val get_partial : t -> string -> (int * int option) list -> bytes list option

  (** Check whether a key exists. *)
  val exists : t -> string -> bool
end

(** Writable store operations *)
module type WRITABLE = sig
  type t

  (** Set the contents of a key, creating or replacing it. *)
  val set : t -> string -> bytes -> unit

  (** Write byte ranges.  Each item is [(key, offset, bytes)]. *)
  val set_partial : t -> (string * int * bytes) list -> unit

  (** Remove a key; removing a missing key is not an error. *)
  val erase : t -> string -> unit

  (** Remove every key with the given prefix. *)
  val erase_prefix : t -> string -> unit
end

(** Listable store operations *)
module type LISTABLE = sig
  type t

  (** All keys in the store. *)
  val list : t -> string list

  (** All keys with the given prefix. *)
  val list_prefix : t -> string -> string list

  (** The direct children of a prefix as [(keys, sub-prefixes)].  [prefix] is
      [""] or ends with ["/"]; the returned sub-prefixes end with ["/"]. *)
  val list_dir : t -> string -> string list * string list
end

(** The complete exception-based store interface. *)
module type STORE = sig
  include READABLE
  include WRITABLE with type t := t
  include LISTABLE with type t := t
end

(** The complete result-based store interface.  [get] distinguishes a missing
    key ([Ok None]) from a failed operation ([Error _]). *)
module type STORE_WITH_ERRORS = sig
  type t
  type error

  val pp_error : Format.formatter -> error -> unit

  val get : t -> string -> (bytes option, error) result
  val get_partial : t -> string -> (int * int option) list -> (bytes list option, error) result
  val exists : t -> string -> (bool, error) result
  val set : t -> string -> bytes -> (unit, error) result
  val set_partial : t -> (string * int * bytes) list -> (unit, error) result
  val erase : t -> string -> (unit, error) result
  val erase_prefix : t -> string -> (unit, error) result
  val list : t -> (string list, error) result
  val list_prefix : t -> string -> (string list, error) result
  val list_dir : t -> string -> (string list * string list, error) result
end

(** Raised by {!Raise_errors} when the underlying store reports an error;
    the payload is the error pretty-printed. *)
exception Store_error of string

(** Adapt a result-based store to the exception-based {!STORE} interface, so
    that it can be given to [Zarr.Array.Make] and [Zarr.Group.Make]. *)
module Raise_errors (S : STORE_WITH_ERRORS) : STORE with type t = S.t = struct
  type t = S.t

  let ok = function
    | Ok v -> v
    | Error e -> raise (Store_error (Format.asprintf "%a" S.pp_error e))

  let get t key = ok (S.get t key)
  let get_partial t key ranges = ok (S.get_partial t key ranges)
  let exists t key = ok (S.exists t key)
  let set t key value = ok (S.set t key value)
  let set_partial t updates = ok (S.set_partial t updates)
  let erase t key = ok (S.erase t key)
  let erase_prefix t prefix = ok (S.erase_prefix t prefix)
  let list t = ok (S.list t)
  let list_prefix t prefix = ok (S.list_prefix t prefix)
  let list_dir t prefix = ok (S.list_dir t prefix)
end

(** Byte ranges of an in-memory value, clamped to its bounds; shared by stores
    that cannot read partially. *)
let sub_ranges bytes ranges =
  let len = Bytes.length bytes in
  List.map (fun (offset, length) ->
    let offset = max 0 (min offset len) in
    let length = match length with Some l -> l | None -> len - offset in
    Bytes.sub bytes offset (max 0 (min length (len - offset)))
  ) ranges

(** Apply partial writes to an in-memory value, growing it with zero bytes as
    needed; shared by stores that cannot write partially. *)
let patch existing ~offset bytes =
  let existing_len = Bytes.length existing in
  let new_len = max existing_len (offset + Bytes.length bytes) in
  let result = Bytes.make new_len '\x00' in
  Bytes.blit existing 0 result 0 existing_len;
  Bytes.blit bytes 0 result offset (Bytes.length bytes);
  result
