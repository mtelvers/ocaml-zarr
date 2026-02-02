(** Gzip codec - zlib/gzip compression using decompress library *)

type bigstring = (char, Bigarray.int8_unsigned_elt, Bigarray.c_layout) Bigarray.Array1.t

(** Create a bigstring from bytes *)
let bigstring_of_bytes (b : bytes) : bigstring =
  let len = Bytes.length b in
  let bs = Bigarray.Array1.create Bigarray.char Bigarray.c_layout len in
  for i = 0 to len - 1 do
    Bigarray.Array1.set bs i (Bytes.get b i)
  done;
  bs

(** Create bytes from a bigstring *)
let bytes_of_bigstring (bs : bigstring) (len : int) : bytes =
  let b = Bytes.create len in
  for i = 0 to len - 1 do
    Bytes.set b i (Bigarray.Array1.get bs i)
  done;
  b

(** Compress bytes using zlib deflate *)
let compress ~level input =
  let input_len = Bytes.length input in
  if input_len = 0 then Bytes.empty
  else begin
    let output = Buffer.create (input_len / 2 + 128) in

    let w = De.Lz77.make_window ~bits:15 in
    let q = De.Queue.create 0x1000 in

    let input_pos = ref 0 in
    let refill (buf : bigstring) =
      let available = input_len - !input_pos in
      let buf_len = Bigarray.Array1.dim buf in
      let len = min buf_len available in
      for i = 0 to len - 1 do
        Bigarray.Array1.set buf i (Bytes.get input (!input_pos + i))
      done;
      input_pos := !input_pos + len;
      len
    in

    let flush (buf : bigstring) len =
      for i = 0 to len - 1 do
        Buffer.add_char output (Bigarray.Array1.get buf i)
      done
    in

    (* Create input and output buffers for the library *)
    let i_buf = Bigarray.Array1.create Bigarray.char Bigarray.c_layout 0x1000 in
    let o_buf = Bigarray.Array1.create Bigarray.char Bigarray.c_layout 0x1000 in

    Zl.Higher.compress ~level ~w ~q ~refill ~flush i_buf o_buf;

    Buffer.to_bytes output
  end

(** Decompress bytes using zlib inflate *)
let decompress input =
  let input_len = Bytes.length input in
  if input_len = 0 then Ok Bytes.empty
  else begin
    try
      let output = Buffer.create (input_len * 4) in

      let allocate bits = De.make_window ~bits in

      let input_pos = ref 0 in
      let refill (buf : bigstring) =
        let available = input_len - !input_pos in
        let buf_len = Bigarray.Array1.dim buf in
        let len = min buf_len available in
        for i = 0 to len - 1 do
          Bigarray.Array1.set buf i (Bytes.get input (!input_pos + i))
        done;
        input_pos := !input_pos + len;
        len
      in

      let flush (buf : bigstring) len =
        for i = 0 to len - 1 do
          Buffer.add_char output (Bigarray.Array1.get buf i)
        done
      in

      (* Create input and output buffers for the library *)
      let i_buf = Bigarray.Array1.create Bigarray.char Bigarray.c_layout 0x1000 in
      let o_buf = Bigarray.Array1.create Bigarray.char Bigarray.c_layout 0x1000 in

      match Zl.Higher.uncompress ~allocate ~refill ~flush i_buf o_buf with
      | Ok () -> Ok (Buffer.to_bytes output)
      | Error (`Msg msg) -> Error (`Codec_error ("gzip decompress failed: " ^ msg))
    with
    | Invalid_argument msg -> Error (`Codec_error ("gzip decompress invalid: " ^ msg))
    | exn -> Error (`Codec_error ("gzip decompress error: " ^ Printexc.to_string exn))
  end

(** Create a gzip codec with specified compression level *)
let create level : Codec_intf.bytes_to_bytes = {
  encode = (fun bytes -> compress ~level bytes);
  decode = (fun bytes -> decompress bytes);
  compute_encoded_size = (fun _ -> None);  (* Compressed size is variable *)
}
