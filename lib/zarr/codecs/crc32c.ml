(** CRC32C codec: appends a little-endian CRC32C (Castagnoli) checksum.
    The checksum itself comes from [checkseum]. *)

let compute bytes =
  Checkseum.Crc32c.(digest_bytes bytes 0 (Bytes.length bytes) default |> to_int32)

(** Append the checksum. *)
let encode bytes =
  let len = Bytes.length bytes in
  let result = Bytes.create (len + 4) in
  Bytes.blit bytes 0 result 0 len;
  Bytes.set_int32_le result len (compute bytes);
  result

(** Verify and strip the checksum. *)
let decode bytes =
  let len = Bytes.length bytes in
  if len < 4 then Error `Checksum_mismatch
  else
    let data = Bytes.sub bytes 0 (len - 4) in
    if Int32.equal (Bytes.get_int32_le bytes (len - 4)) (compute data) then Ok data
    else Error `Checksum_mismatch

let create () : Codec_intf.bytes_to_bytes = {
  encode;
  decode;
  compute_encoded_size = (fun size -> Some (size + 4));
}
