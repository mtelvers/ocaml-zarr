(** Slice normalisation and chunk intersection for array indexing *)

open Ztypes

(** A normalised, half-open, positively stepped range along one axis. *)
type range = { start : int; stop : int; step : int }

(** Number of elements a range selects. *)
let length r = if r.stop <= r.start then 0 else (r.stop - r.start + r.step - 1) / r.step

(** Normalise one slice against a dimension of [dim_size]: negative indices
    count from the end, ranges are clamped, steps must be positive. *)
let normalize_slice dim_size slice =
  let wrap i = if i < 0 then dim_size + i else i in
  let clamp i = max 0 (min i dim_size) in
  match slice with
  | Index i ->
    let i = wrap i in
    if i < 0 || i >= dim_size then
      Error (`Invalid_slice (Printf.sprintf "index %d out of bounds for size %d" i dim_size))
    else Ok { start = i; stop = i + 1; step = 1 }
  | Range (start, stop) ->
    let start = clamp (wrap start) and stop = clamp (wrap stop) in
    if start >= stop then
      Error (`Invalid_slice (Printf.sprintf "invalid range [%d:%d)" start stop))
    else Ok { start; stop; step = 1 }
  | RangeFrom start -> Ok { start = clamp (wrap start); stop = dim_size; step = 1 }
  | RangeTo stop -> Ok { start = 0; stop = clamp (wrap stop); step = 1 }
  | All -> Ok { start = 0; stop = dim_size; step = 1 }
  | Stepped (start, stop, step) ->
    if step <= 0 then Error (`Invalid_slice "step must be positive")
    else Ok { start = clamp (wrap start); stop = clamp (wrap stop); step }

(** Normalise a slice list against an array shape.  Missing trailing slices
    select whole dimensions; extra slices are an error. *)
let normalize shape slices =
  let ndim = Array.length shape in
  let n = List.length slices in
  if n > ndim then
    Error (`Invalid_slice (Printf.sprintf "%d slices given for a %d-dimensional array" n ndim))
  else
    let slices = slices @ List.init (ndim - n) (fun _ -> All) in
    List.fold_left (fun acc (i, slice) ->
      match acc with
      | Error _ -> acc
      | Ok ranges ->
        Result.map (fun r -> r :: ranges) (normalize_slice shape.(i) slice)
    ) (Ok []) (List.mapi (fun i s -> (i, s)) slices)
    |> Result.map (fun l -> Array.of_list (List.rev l))

(** Shape of the selection made by normalised ranges. *)
let shape_of_ranges ranges = Array.map length ranges

(** Shape of the selection made by [slices], or [[||]] if they are invalid. *)
let output_shape shape slices =
  match normalize shape slices with Ok r -> shape_of_ranges r | Error _ -> [||]

(** The part of a selection that falls in one chunk, along one axis. *)
type overlap = {
  chunk_offset : int;  (** first selected element, relative to the chunk *)
  out_offset : int;    (** its position in the selection *)
  count : int;         (** number of selected elements in the chunk *)
  step : int;
}

(** For each axis, the overlap of [ranges] with the chunk at [chunk_coords]. *)
let chunk_overlap ~chunk_shape ~array_shape chunk_coords ranges =
  Array.mapi (fun d r ->
    let cs = chunk_shape.(d) in
    let chunk_start = chunk_coords.(d) * cs in
    let chunk_end = min (chunk_start + cs) array_shape.(d) in
    let n = length r in
    (* k-th selected element is at r.start + k * r.step *)
    let ceil_div a b = (a + b - 1) / b in
    let k0 = if chunk_start <= r.start then 0 else ceil_div (chunk_start - r.start) r.step in
    let k1 = if chunk_end <= r.start then 0 else min n (ceil_div (chunk_end - r.start) r.step) in
    let count = max 0 (k1 - k0) in
    { chunk_offset = r.start + k0 * r.step - chunk_start; out_offset = k0; count; step = r.step }
  ) ranges

(** Coordinates of every chunk that holds at least one selected element, in
    C order. *)
let chunks_for_ranges ~chunk_shape ~array_shape ranges =
  let ndim = Array.length ranges in
  if Array.exists (fun r -> length r = 0) ranges then []
  else begin
    let first = Array.mapi (fun d r -> r.start / chunk_shape.(d)) ranges in
    let last = Array.mapi (fun d r -> (r.start + (length r - 1) * r.step) / chunk_shape.(d)) ranges in
    let acc = ref [] in
    let current = Array.copy first in
    let rec go d =
      if d = ndim then begin
        let ov = chunk_overlap ~chunk_shape ~array_shape current ranges in
        if Array.for_all (fun o -> o.count > 0) ov then acc := Array.copy current :: !acc
      end else
        for c = first.(d) to last.(d) do
          current.(d) <- c;
          go (d + 1)
        done
    in
    go 0;
    List.rev !acc
  end
