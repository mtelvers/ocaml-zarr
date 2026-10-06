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
  multipart_threshold : int;
  part_size : int;
  upload_concurrency : int;
  delete_concurrency : int;
}

type error = S3.Client.error

let pp_error = S3.Client.pp_error

(** [create ?prefix ?max_object_size ?multipart_threshold ?part_size
    ?upload_concurrency ?delete_concurrency client ~bucket] is a store rooted
    at [prefix] in [bucket].

    @param max_object_size the largest object [get] will read, in bytes
      (default 16 GiB; shards can be large).
    @param multipart_threshold values larger than this are uploaded in parts
      (default 16 MiB).
    @param part_size size of the parts a large value is uploaded in
      (default 64 MiB, so a 2 GiB shard is 32 parts).
    @param upload_concurrency parts of one value uploaded at once (default 4).
    @param delete_concurrency objects deleted at once by [erase_prefix]
      (default 16). *)
let create ?(prefix = "") ?(max_object_size = 1 lsl 34) ?(multipart_threshold = 16 * 1024 * 1024)
    ?(part_size = 64 * 1024 * 1024) ?(upload_concurrency = 4) ?(delete_concurrency = 16)
    client ~bucket =
  let prefix =
    if prefix = "" || String.ends_with ~suffix:"/" prefix then prefix else prefix ^ "/"
  in
  { client; bucket; prefix; max_object_size; multipart_threshold; part_size;
    upload_concurrency; delete_concurrency }

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

(** Values up to [multipart_threshold] go up as one PUT; larger
    ones, such as shards, as a multipart upload of [part_size] parts with
    independent retries.  The bytes are read through a flow without first
    being copied whole ([value] is not used again, so the string view is
    safe); only [part_size * upload_concurrency] bytes are buffered at once. *)
let set t key value =
  let size = Bytes.length value in
  Result.map ignore
    (S3.Client.put_flow t.client ~bucket:t.bucket ~key:(object_key t key)
       ~multipart_threshold:t.multipart_threshold ~part_size:t.part_size
       ~max_concurrency:t.upload_concurrency ~size
       (Eio.Flow.string_source (Bytes.unsafe_to_string value)))

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

(** Direct children of a prefix, from a delimiter listing: one request per
    1000 children, however many objects lie beneath them. *)
let list_dir t prefix =
  let* keys, dirs =
    S3.Client.fold_pages t.client ~bucket:t.bucket ~prefix:(object_key t prefix) ~delimiter:"/"
      ~init:([], [])
      ~f:(fun (keys, dirs) page ->
        ( List.rev_append (List.map (fun (e : S3.Client.entry) -> strip_prefix t e.key) page.objects) keys,
          List.rev_append (List.map (strip_prefix t) page.common_prefixes) dirs ))
      ()
  in
  Ok (List.sort String.compare keys, List.sort String.compare dirs)
