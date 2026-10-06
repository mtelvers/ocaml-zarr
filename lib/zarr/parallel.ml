(** Minimal fork-join parallelism over OCaml 5 domains.

    Used by the sharding codec to compress and decompress inner chunks
    concurrently.  Work is handed out dynamically through a shared counter so
    that cheap items (empty chunks) and expensive ones balance across domains. *)

(** [iter ~domains n f] calls [f i] for every [0 <= i < n], using up to
    [domains] domains including the caller's.  Exceptions raised by [f] stop
    the remaining work and the first one is re-raised once all domains have
    finished. *)
let iter ~domains n f =
  let domains = min domains n in
  if domains <= 1 then
    for i = 0 to n - 1 do f i done
  else begin
    let next = Atomic.make 0 in
    let worker () =
      let rec go () =
        let i = Atomic.fetch_and_add next 1 in
        if i < n then begin
          (match f i with
           | () -> ()
           | exception e ->
             Atomic.set next n;  (* stop the other workers at their next fetch *)
             raise e);
          go ()
        end
      in
      match go () with
      | () -> None
      | exception e -> Some e
    in
    let others = List.init (domains - 1) (fun _ -> Domain.spawn worker) in
    let mine = worker () in
    let failures = List.filter_map Domain.join others in
    match mine, failures with
    | Some e, _ | None, e :: _ -> raise e
    | None, [] -> ()
  end
