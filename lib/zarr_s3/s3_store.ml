(** A Zarr store on an S3 bucket, implementing {!Zarr.Store.STORE_WITH_ERRORS}
    over [S3.Client].

    Keys map to objects at [prefix ^ key] in one bucket.  Every operation is
    a blocking Eio call that returns the S3 client's error on failure; a
    missing object is [Ok None] / [Ok false], never an error. *)

type t = {
  client : S3.Client.t;
  bucket : string;
  prefix : string;  (** [""] or ending in ["/"] *)
  max_object_size : int;
  delete_concurrency : int;
}

type error = S3.Client.error

let pp_error = S3.Client.pp_error

(** [create ?prefix ?max_object_size ?delete_concurrency client ~bucket] is a
    store rooted at [prefix] in [bucket].

    @param max_object_size the largest object [get] will read, in bytes
      (default 16 GiB; shards can be large).
    @param delete_concurrency objects deleted at once by [erase_prefix]
      (default 16). *)
let create ?(prefix = "") ?(max_object_size = 1 lsl 34) ?(delete_concurrency = 16) client ~bucket =
  let prefix =
    if prefix = "" || String.ends_with ~suffix:"/" prefix then prefix else prefix ^ "/"
  in
  { client; bucket; prefix; max_object_size; delete_concurrency }

let bucket t = t.bucket
let prefix t = t.prefix

let object_key t key = t.prefix ^ key

let strip_prefix t key =
  let n = String.length t.prefix in
  if String.starts_with ~prefix:t.prefix key then String.sub key n (String.length key - n) else key

let is_not_found (e : error) =
  e.http_status = 404 || e.code = "NoSuchKey" || e.code = "NotFound"

let ( let* ) = Result.bind

(** Collect a list of results into a result of a list, stopping at the first error. *)
let all results =
  List.fold_right (fun r acc -> let* acc = acc in let* v = r in Ok (v :: acc)) results (Ok [])

let get t key =
  match S3.Client.get_string t.client ~bucket:t.bucket ~key:(object_key t key)
          ~max_size:t.max_object_size () with
  | Ok s -> Ok (Some (Bytes.of_string s))
  | Error e when is_not_found e -> Ok None
  | Error e -> Error e

let head t key =
  match S3.Client.head_object t.client ~bucket:t.bucket ~key:(object_key t key) with
  | Ok md -> Ok (Some md)
  | Error e when is_not_found e -> Ok None
  | Error e -> Error e

(** Byte ranges are read with HTTP range requests, one per range; an
    open-ended range first needs the object's size. *)
let get_partial t key ranges =
  let key' = object_key t key in
  let* size =
    if List.exists (fun (_, len) -> len = None) ranges then
      let* md = head t key in
      Ok (Option.map (fun (md : S3.Client.metadata) -> md.content_length) md)
    else Ok (Some 0)
  in
  match size with
  | None -> Ok None
  | Some size ->
    let fetch (offset, len) =
      let len = match len with Some l -> l | None -> max 0 (size - offset) in
      if len <= 0 then Ok Bytes.empty
      else
        Result.map Bytes.of_string
          (S3.Client.get_range t.client ~bucket:t.bucket ~key:key' ~first:offset ~last:(offset + len - 1) ())
    in
    match all (List.map fetch ranges) with
    | Ok parts -> Ok (Some parts)
    | Error e when is_not_found e -> Ok None
    | Error e -> Error e

let exists t key = Result.map Option.is_some (head t key)

(** One PUT of the whole value.  The bytes are handed to the client without
    copying ([value] is not used again, so the string view is safe), but a
    shard still travels as a single object: a transport failure re-sends all
    of it, and the 5 GiB single-PUT limit applies.  Multipart upload from
    memory needs an [S3.Client] entry point that takes a string or flow; the
    client's multipart path currently only reads from a file. *)
let set t key value =
  Result.map ignore
    (S3.Client.put_string t.client ~bucket:t.bucket ~key:(object_key t key)
       (Bytes.unsafe_to_string value))

(** S3 objects are immutable, so partial writes are read-modify-write. *)
let set_partial t updates =
  List.fold_left (fun acc (key, offset, bytes) ->
    let* () = acc in
    let* existing = get t key in
    set t key (Zarr.Store.patch (Option.value ~default:Bytes.empty existing) ~offset bytes)
  ) (Ok ()) updates

let erase t key =
  match S3.Client.delete_object t.client ~bucket:t.bucket ~key:(object_key t key) with
  | Ok () -> Ok ()
  | Error e when is_not_found e -> Ok ()
  | Error e -> Error e

let list_prefix t prefix =
  S3.Client.list_objects t.client ~bucket:t.bucket ~prefix:(object_key t prefix) ()
  |> Result.map (List.map (strip_prefix t))

let list t = list_prefix t ""

let erase_prefix t prefix =
  let* keys = list_prefix t prefix in
  Eio.Fiber.List.map ~max_fibers:t.delete_concurrency (erase t) keys
  |> all |> Result.map ignore

(** Direct children of a prefix.  The client lists without a delimiter, so this
    walks every object under the prefix and groups them; on a large array
    prefix that is one request per 1000 chunks. *)
let list_dir t prefix =
  let full = object_key t prefix in
  let n = String.length full in
  let* keys, dirs =
    S3.Client.fold_pages t.client ~bucket:t.bucket ~prefix:full ~init:([], [])
      ~f:(fun (keys, dirs) page ->
        List.fold_left (fun (keys, dirs) (e : S3.Client.entry) ->
          match String.index_from_opt e.key n '/' with
          | None -> (strip_prefix t e.key :: keys, dirs)
          | Some i ->
            let dir = strip_prefix t (String.sub e.key 0 (i + 1)) in
            (keys, if List.mem dir dirs then dirs else dir :: dirs)
        ) (keys, dirs) page.objects)
      ()
  in
  Ok (List.sort String.compare keys, List.sort String.compare dirs)
