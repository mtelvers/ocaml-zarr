(** N-dimensional arrays backed by [Bigarray.Genarray] in C (row-major) layout.

    Every array is a contiguous block of elements of one of the supported
    Bigarray kinds.  The constructors of {!t} are public so that callers can
    reach the underlying bigarray when they need to; the functions in this
    module cover the common operations without exposing the kind. *)

open Bigarray

module D = Ztypes.Dtype
module FV = Ztypes.Fill_value
module E = Ztypes.Endianness

type ('a, 'b) ga = ('a, 'b, c_layout) Genarray.t

(** Existential wrapper over the Bigarray kinds used for Zarr data types.
    [Uint32] and [Uint64] are stored as [Int64]; [Float16] as [Float32];
    [Bool] as [Int8_unsigned]; raw [r8] as [Char]. *)
type t =
  | Int8_signed of (int, int8_signed_elt) ga
  | Int8_unsigned of (int, int8_unsigned_elt) ga
  | Int16_signed of (int, int16_signed_elt) ga
  | Int16_unsigned of (int, int16_unsigned_elt) ga
  | Int32 of (int32, int32_elt) ga
  | Int64 of (int64, int64_elt) ga
  | Float32 of (float, float32_elt) ga
  | Float64 of (float, float64_elt) ga
  | Complex32 of (Complex.t, complex32_elt) ga
  | Complex64 of (Complex.t, complex64_elt) ga
  | Char of (char, int8_unsigned_elt) ga

(** {1 Kind-generic plumbing} *)

type 'r kind_fn = { f : 'a 'b. ('a, 'b) ga -> 'r }
type kind_map = { g : 'a 'b. ('a, 'b) ga -> ('a, 'b) ga }
type 'r kind_fn2 = { f2 : 'a 'b. ('a, 'b) ga -> ('a, 'b) ga -> 'r }

let apply { f } = function
  | Int8_signed a -> f a
  | Int8_unsigned a -> f a
  | Int16_signed a -> f a
  | Int16_unsigned a -> f a
  | Int32 a -> f a
  | Int64 a -> f a
  | Float32 a -> f a
  | Float64 a -> f a
  | Complex32 a -> f a
  | Complex64 a -> f a
  | Char a -> f a

let map { g } = function
  | Int8_signed a -> Int8_signed (g a)
  | Int8_unsigned a -> Int8_unsigned (g a)
  | Int16_signed a -> Int16_signed (g a)
  | Int16_unsigned a -> Int16_unsigned (g a)
  | Int32 a -> Int32 (g a)
  | Int64 a -> Int64 (g a)
  | Float32 a -> Float32 (g a)
  | Float64 a -> Float64 (g a)
  | Complex32 a -> Complex32 (g a)
  | Complex64 a -> Complex64 (g a)
  | Char a -> Char (g a)

(** Apply a binary operation to two arrays of the same kind. *)
let apply2 { f2 } a b =
  match a, b with
  | Int8_signed x, Int8_signed y -> f2 x y
  | Int8_unsigned x, Int8_unsigned y -> f2 x y
  | Int16_signed x, Int16_signed y -> f2 x y
  | Int16_unsigned x, Int16_unsigned y -> f2 x y
  | Int32 x, Int32 y -> f2 x y
  | Int64 x, Int64 y -> f2 x y
  | Float32 x, Float32 y -> f2 x y
  | Float64 x, Float64 y -> f2 x y
  | Complex32 x, Complex32 y -> f2 x y
  | Complex64 x, Complex64 y -> f2 x y
  | Char x, Char y -> f2 x y
  | _ -> invalid_arg "Ndarray: arrays have different element kinds"

let numel_of_dims dims = Array.fold_left ( * ) 1 dims

(** C-order strides: the element distance between neighbours along each axis. *)
let strides dims =
  let n = Array.length dims in
  let s = Array.make n 1 in
  for i = n - 2 downto 0 do
    s.(i) <- s.(i + 1) * dims.(i + 1)
  done;
  s

(** A flat 1-D view sharing storage with [a]. *)
let flat (type a b) (a : (a, b) ga) : (a, b, c_layout) Array1.t =
  reshape_1 a (numel_of_dims (Genarray.dims a))

(** {1 Shape queries} *)

let shape = apply { f = Genarray.dims }
let ndim arr = Array.length (shape arr)
let numel arr = numel_of_dims (shape arr)

let dtype = function
  | Int8_signed _ -> D.Int8
  | Int8_unsigned _ -> D.Uint8
  | Int16_signed _ -> D.Int16
  | Int16_unsigned _ -> D.Uint16
  | Int32 _ -> D.Int32
  | Int64 _ -> D.Int64
  | Float32 _ -> D.Float32
  | Float64 _ -> D.Float64
  | Complex32 _ -> D.Complex64
  | Complex64 _ -> D.Complex128
  | Char _ -> D.Raw 8

let dtype_of_ndarray = dtype

(** {1 Construction} *)

(** An array whose contents are uninitialised. *)
let empty dtype dims =
  match dtype with
  | D.Bool | D.Uint8 -> Int8_unsigned (Genarray.create int8_unsigned c_layout dims)
  | D.Int8 -> Int8_signed (Genarray.create int8_signed c_layout dims)
  | D.Int16 -> Int16_signed (Genarray.create int16_signed c_layout dims)
  | D.Uint16 -> Int16_unsigned (Genarray.create int16_unsigned c_layout dims)
  | D.Int32 -> Int32 (Genarray.create int32 c_layout dims)
  | D.Int64 | D.Uint32 | D.Uint64 -> Int64 (Genarray.create int64 c_layout dims)
  | D.Float16 | D.Float32 -> Float32 (Genarray.create float32 c_layout dims)
  | D.Float64 -> Float64 (Genarray.create float64 c_layout dims)
  | D.Complex64 -> Complex32 (Genarray.create complex32 c_layout dims)
  | D.Complex128 -> Complex64 (Genarray.create complex64 c_layout dims)
  | D.Raw _ -> Char (Genarray.create char c_layout dims)

let zero = function
  | Int8_signed a -> Genarray.fill a 0
  | Int8_unsigned a -> Genarray.fill a 0
  | Int16_signed a -> Genarray.fill a 0
  | Int16_unsigned a -> Genarray.fill a 0
  | Int32 a -> Genarray.fill a 0l
  | Int64 a -> Genarray.fill a 0L
  | Float32 a -> Genarray.fill a 0.0
  | Float64 a -> Genarray.fill a 0.0
  | Complex32 a -> Genarray.fill a Complex.zero
  | Complex64 a -> Genarray.fill a Complex.zero
  | Char a -> Genarray.fill a '\x00'

(** A zero-filled array. *)
let create dtype dims =
  let arr = empty dtype dims in
  zero arr;
  arr

(** The float a fill value denotes, for the float data types. *)
let float_of_fill = function
  | FV.Float f -> Some f
  | FV.NaN -> Some Float.nan
  | FV.Infinity -> Some Float.infinity
  | FV.NegInfinity -> Some Float.neg_infinity
  | FV.Hex s ->
    (match Int64.of_string_opt s with
     | Some bits when String.length s > 10 -> Some (Int64.float_of_bits bits)
     | Some bits -> Some (Int32.float_of_bits (Int64.to_int32 bits))
     | None -> None)
  | FV.Int i -> Some (Int64.to_float i)
  | _ -> None

(** Fill [arr] with [value]; returns [false] if the value does not apply to
    the array's element kind, in which case the array is left untouched. *)
let fill_with arr value =
  match arr, value with
  | Int8_signed a, FV.Int i -> Genarray.fill a (Int64.to_int i); true
  | Int8_unsigned a, FV.Uint u -> Genarray.fill a (Int64.to_int u); true
  | Int8_unsigned a, FV.Bool b -> Genarray.fill a (Bool.to_int b); true
  | Int16_signed a, FV.Int i -> Genarray.fill a (Int64.to_int i); true
  | Int16_unsigned a, FV.Uint u -> Genarray.fill a (Int64.to_int u); true
  | Int32 a, FV.Int i -> Genarray.fill a (Int64.to_int32 i); true
  | Int64 a, (FV.Int i | FV.Uint i) -> Genarray.fill a i; true
  | Float32 a, fv ->
    (match float_of_fill fv with Some f -> Genarray.fill a f; true | None -> false)
  | Float64 a, fv ->
    (match float_of_fill fv with Some f -> Genarray.fill a f; true | None -> false)
  | Complex32 a, FV.Complex (re, im) -> Genarray.fill a { Complex.re; im }; true
  | Complex64 a, FV.Complex (re, im) -> Genarray.fill a { Complex.re; im }; true
  | Char a, FV.Raw b when Bytes.length b >= 1 -> Genarray.fill a (Bytes.get b 0); true
  | _ -> false

(** Fill an array with a constant; a value of the wrong kind is ignored. *)
let fill arr value = ignore (fill_with arr value)

(** An array filled with [fill_value] (or zeros if it does not apply). *)
let make dtype dims fill_value =
  let arr = empty dtype dims in
  if not (fill_with arr fill_value) then zero arr;
  arr

(** {1 Element-wise predicates} *)

(* One loop per kind: Bigarray accessors are only specialised (unboxed, no C
   call) when the kind is statically known, so these cannot share a body. *)

let all_i8s (a : (int, int8_signed_elt) ga) v =
  let a = flat a in
  let n = Array1.dim a in
  let rec go i = i >= n || (Array1.unsafe_get a i = v && go (i + 1)) in
  go 0

let all_i8u (a : (int, int8_unsigned_elt) ga) v =
  let a = flat a in
  let n = Array1.dim a in
  let rec go i = i >= n || (Array1.unsafe_get a i = v && go (i + 1)) in
  go 0

let all_i16s (a : (int, int16_signed_elt) ga) v =
  let a = flat a in
  let n = Array1.dim a in
  let rec go i = i >= n || (Array1.unsafe_get a i = v && go (i + 1)) in
  go 0

let all_i16u (a : (int, int16_unsigned_elt) ga) v =
  let a = flat a in
  let n = Array1.dim a in
  let rec go i = i >= n || (Array1.unsafe_get a i = v && go (i + 1)) in
  go 0

let all_i32 (a : (int32, int32_elt) ga) v =
  let a = flat a in
  let n = Array1.dim a in
  let rec go i = i >= n || (Int32.equal (Array1.unsafe_get a i) v && go (i + 1)) in
  go 0

let all_i64 (a : (int64, int64_elt) ga) v =
  let a = flat a in
  let n = Array1.dim a in
  let rec go i = i >= n || (Int64.equal (Array1.unsafe_get a i) v && go (i + 1)) in
  go 0

(* [Float.equal] treats NaN as equal to itself, matching numpy's
   [array_equal(..., equal_nan=True)] used by zarr-python. *)
let all_f32 (a : (float, float32_elt) ga) v =
  let a = flat a in
  let n = Array1.dim a in
  let rec go i = i >= n || (Float.equal (Array1.unsafe_get a i) v && go (i + 1)) in
  go 0

let all_f64 (a : (float, float64_elt) ga) v =
  let a = flat a in
  let n = Array1.dim a in
  let rec go i = i >= n || (Float.equal (Array1.unsafe_get a i) v && go (i + 1)) in
  go 0

let complex_equal (x : Complex.t) (y : Complex.t) =
  Float.equal x.re y.re && Float.equal x.im y.im

let all_c32 (a : (Complex.t, complex32_elt) ga) v =
  let a = flat a in
  let n = Array1.dim a in
  let rec go i = i >= n || (complex_equal (Array1.unsafe_get a i) v && go (i + 1)) in
  go 0

let all_c64 (a : (Complex.t, complex64_elt) ga) v =
  let a = flat a in
  let n = Array1.dim a in
  let rec go i = i >= n || (complex_equal (Array1.unsafe_get a i) v && go (i + 1)) in
  go 0

let all_char (a : (char, int8_unsigned_elt) ga) v =
  let a = flat a in
  let n = Array1.dim a in
  let rec go i = i >= n || (Char.equal (Array1.unsafe_get a i) v && go (i + 1)) in
  go 0

(** [equals_fill arr fv] is [true] iff every element of [arr] equals [fv].
    NaN equals NaN.  A fill value that does not apply to the array's kind
    never matches. *)
let equals_fill arr fv =
  match arr, fv with
  | Int8_signed a, FV.Int i -> all_i8s a (Int64.to_int i)
  | Int8_unsigned a, FV.Uint u -> all_i8u a (Int64.to_int u)
  | Int8_unsigned a, FV.Bool b -> all_i8u a (Bool.to_int b)
  | Int16_signed a, FV.Int i -> all_i16s a (Int64.to_int i)
  | Int16_unsigned a, FV.Uint u -> all_i16u a (Int64.to_int u)
  | Int32 a, FV.Int i -> all_i32 a (Int64.to_int32 i)
  | Int64 a, (FV.Int i | FV.Uint i) -> all_i64 a i
  | Float32 a, fv ->
    (match float_of_fill fv with Some f -> all_f32 a f | None -> false)
  | Float64 a, fv ->
    (match float_of_fill fv with Some f -> all_f64 a f | None -> false)
  | Complex32 a, FV.Complex (re, im) -> all_c32 a { Complex.re; im }
  | Complex64 a, FV.Complex (re, im) -> all_c64 a { Complex.re; im }
  | Char a, FV.Raw b when Bytes.length b >= 1 -> all_char a (Bytes.get b 0)
  | _ -> false

(** {1 Index arithmetic} *)

(** Linear offset of a multi-index in C order. *)
let index_to_offset dims idx =
  let offset = ref 0 and stride = ref 1 in
  for i = Array.length dims - 1 downto 0 do
    offset := !offset + idx.(i) * !stride;
    stride := !stride * dims.(i)
  done;
  !offset

(** Multi-index of a linear offset in C order. *)
let offset_to_index dims offset =
  let ndim = Array.length dims in
  let idx = Array.make ndim 0 in
  let remaining = ref offset in
  for i = ndim - 1 downto 0 do
    idx.(i) <- !remaining mod dims.(i);
    remaining := !remaining / dims.(i)
  done;
  idx

(** {1 Scalar access} *)

(** Scalar values, boxed as polymorphic variants. *)
type scalar =
  [ `Int of int
  | `Int32 of int32
  | `Int64 of int64
  | `Float of float
  | `Complex of Complex.t
  | `Char of char ]

let get arr idx : scalar =
  match arr with
  | Int8_signed a -> `Int (Genarray.get a idx)
  | Int8_unsigned a -> `Int (Genarray.get a idx)
  | Int16_signed a -> `Int (Genarray.get a idx)
  | Int16_unsigned a -> `Int (Genarray.get a idx)
  | Int32 a -> `Int32 (Genarray.get a idx)
  | Int64 a -> `Int64 (Genarray.get a idx)
  | Float32 a -> `Float (Genarray.get a idx)
  | Float64 a -> `Float (Genarray.get a idx)
  | Complex32 a -> `Complex (Genarray.get a idx)
  | Complex64 a -> `Complex (Genarray.get a idx)
  | Char a -> `Char (Genarray.get a idx)

(** Set an element; a value of the wrong kind is ignored. *)
let set arr idx (value : scalar) =
  match arr, value with
  | Int8_signed a, `Int v -> Genarray.set a idx v
  | Int8_unsigned a, `Int v -> Genarray.set a idx v
  | Int16_signed a, `Int v -> Genarray.set a idx v
  | Int16_unsigned a, `Int v -> Genarray.set a idx v
  | Int32 a, `Int32 v -> Genarray.set a idx v
  | Int64 a, `Int64 v -> Genarray.set a idx v
  | Float32 a, `Float v -> Genarray.set a idx v
  | Float64 a, `Float v -> Genarray.set a idx v
  | Complex32 a, `Complex v -> Genarray.set a idx v
  | Complex64 a, `Complex v -> Genarray.set a idx v
  | Char a, `Char v -> Genarray.set a idx v
  | _ -> ()

(** {1 Byte serialisation} *)

let elt_size = function
  | Int8_signed _ | Int8_unsigned _ | Char _ -> 1
  | Int16_signed _ | Int16_unsigned _ -> 2
  | Int32 _ | Float32 _ -> 4
  | Int64 _ | Float64 _ | Complex32 _ -> 8
  | Complex64 _ -> 16

(** Serialise in C order with the given byte order. *)
let to_bytes endian arr =
  let n = numel arr in
  let buf = Bytes.create (n * elt_size arr) in
  let le = (endian = E.Little) in
  (match arr with
   | Int8_signed a ->
     let a = flat a in
     for i = 0 to n - 1 do
       Bytes.unsafe_set buf i (Char.unsafe_chr (Array1.unsafe_get a i land 0xFF))
     done
   | Int8_unsigned a ->
     let a = flat a in
     for i = 0 to n - 1 do
       Bytes.unsafe_set buf i (Char.unsafe_chr (Array1.unsafe_get a i))
     done
   | Char a ->
     let a = flat a in
     for i = 0 to n - 1 do
       Bytes.unsafe_set buf i (Array1.unsafe_get a i)
     done
   | Int16_signed a ->
     let a = flat a in
     for i = 0 to n - 1 do
       let v = Array1.unsafe_get a i in
       if le then Bytes.set_int16_le buf (i * 2) v else Bytes.set_int16_be buf (i * 2) v
     done
   | Int16_unsigned a ->
     let a = flat a in
     for i = 0 to n - 1 do
       let v = Array1.unsafe_get a i in
       if le then Bytes.set_uint16_le buf (i * 2) v else Bytes.set_uint16_be buf (i * 2) v
     done
   | Int32 a ->
     let a = flat a in
     for i = 0 to n - 1 do
       let v = Array1.unsafe_get a i in
       if le then Bytes.set_int32_le buf (i * 4) v else Bytes.set_int32_be buf (i * 4) v
     done
   | Int64 a ->
     let a = flat a in
     for i = 0 to n - 1 do
       let v = Array1.unsafe_get a i in
       if le then Bytes.set_int64_le buf (i * 8) v else Bytes.set_int64_be buf (i * 8) v
     done
   | Float32 a ->
     let a = flat a in
     for i = 0 to n - 1 do
       let v = Int32.bits_of_float (Array1.unsafe_get a i) in
       if le then Bytes.set_int32_le buf (i * 4) v else Bytes.set_int32_be buf (i * 4) v
     done
   | Float64 a ->
     let a = flat a in
     for i = 0 to n - 1 do
       let v = Int64.bits_of_float (Array1.unsafe_get a i) in
       if le then Bytes.set_int64_le buf (i * 8) v else Bytes.set_int64_be buf (i * 8) v
     done
   | Complex32 a ->
     let a = flat a in
     for i = 0 to n - 1 do
       let c = Array1.unsafe_get a i in
       let re = Int32.bits_of_float c.re and im = Int32.bits_of_float c.im in
       if le then begin
         Bytes.set_int32_le buf (i * 8) re; Bytes.set_int32_le buf (i * 8 + 4) im
       end else begin
         Bytes.set_int32_be buf (i * 8) re; Bytes.set_int32_be buf (i * 8 + 4) im
       end
     done
   | Complex64 a ->
     let a = flat a in
     for i = 0 to n - 1 do
       let c = Array1.unsafe_get a i in
       let re = Int64.bits_of_float c.re and im = Int64.bits_of_float c.im in
       if le then begin
         Bytes.set_int64_le buf (i * 16) re; Bytes.set_int64_le buf (i * 16 + 8) im
       end else begin
         Bytes.set_int64_be buf (i * 16) re; Bytes.set_int64_be buf (i * 16 + 8) im
       end
     done);
  buf

(** Deserialise from C-order bytes with the given byte order.
    Raises [Invalid_argument] if the buffer is shorter than the array needs. *)
let of_bytes dtype endian dims bytes =
  let arr = empty dtype dims in
  let n = numel arr in
  let need = n * elt_size arr in
  if Bytes.length bytes < need then
    invalid_arg (Printf.sprintf "Ndarray.of_bytes: need %d bytes, got %d" need (Bytes.length bytes));
  let le = (endian = E.Little) in
  (match arr with
   | Int8_signed a ->
     let a = flat a in
     for i = 0 to n - 1 do Array1.unsafe_set a i (Bytes.get_int8 bytes i) done
   | Int8_unsigned a ->
     let a = flat a in
     for i = 0 to n - 1 do Array1.unsafe_set a i (Bytes.get_uint8 bytes i) done
   | Char a ->
     let a = flat a in
     for i = 0 to n - 1 do Array1.unsafe_set a i (Bytes.unsafe_get bytes i) done
   | Int16_signed a ->
     let a = flat a in
     for i = 0 to n - 1 do
       Array1.unsafe_set a i
         (if le then Bytes.get_int16_le bytes (i * 2) else Bytes.get_int16_be bytes (i * 2))
     done
   | Int16_unsigned a ->
     let a = flat a in
     for i = 0 to n - 1 do
       Array1.unsafe_set a i
         (if le then Bytes.get_uint16_le bytes (i * 2) else Bytes.get_uint16_be bytes (i * 2))
     done
   | Int32 a ->
     let a = flat a in
     for i = 0 to n - 1 do
       Array1.unsafe_set a i
         (if le then Bytes.get_int32_le bytes (i * 4) else Bytes.get_int32_be bytes (i * 4))
     done
   | Int64 a ->
     let a = flat a in
     for i = 0 to n - 1 do
       Array1.unsafe_set a i
         (if le then Bytes.get_int64_le bytes (i * 8) else Bytes.get_int64_be bytes (i * 8))
     done
   | Float32 a ->
     let a = flat a in
     for i = 0 to n - 1 do
       let bits = if le then Bytes.get_int32_le bytes (i * 4) else Bytes.get_int32_be bytes (i * 4) in
       Array1.unsafe_set a i (Int32.float_of_bits bits)
     done
   | Float64 a ->
     let a = flat a in
     for i = 0 to n - 1 do
       let bits = if le then Bytes.get_int64_le bytes (i * 8) else Bytes.get_int64_be bytes (i * 8) in
       Array1.unsafe_set a i (Int64.float_of_bits bits)
     done
   | Complex32 a ->
     let a = flat a in
     for i = 0 to n - 1 do
       let re, im =
         if le then Bytes.get_int32_le bytes (i * 8), Bytes.get_int32_le bytes (i * 8 + 4)
         else Bytes.get_int32_be bytes (i * 8), Bytes.get_int32_be bytes (i * 8 + 4)
       in
       Array1.unsafe_set a i { Complex.re = Int32.float_of_bits re; im = Int32.float_of_bits im }
     done
   | Complex64 a ->
     let a = flat a in
     for i = 0 to n - 1 do
       let re, im =
         if le then Bytes.get_int64_le bytes (i * 16), Bytes.get_int64_le bytes (i * 16 + 8)
         else Bytes.get_int64_be bytes (i * 16), Bytes.get_int64_be bytes (i * 16 + 8)
       in
       Array1.unsafe_set a i { Complex.re = Int64.float_of_bits re; im = Int64.float_of_bits im }
     done);
  arr

(** {1 Views} *)

(** [sub_left arr start len] is a view of [len] consecutive hyperslabs along
    the first dimension, sharing storage with [arr]. *)
let sub_left arr start len = map { g = (fun a -> Genarray.sub_left a start len) } arr

(** A view with a new shape of the same element count, sharing storage. *)
let reshape arr new_dims =
  if numel arr <> numel_of_dims new_dims then
    invalid_arg "Ndarray.reshape: incompatible dimensions";
  map { g = (fun a -> reshape a new_dims) } arr

(** {1 Block copies} *)

(* Row copies, one per kind, for the same reason as the predicates above.
   Both parameters must be annotated: Bigarray access is only specialised when
   the kind and layout are known at the point of the access. *)

let row_i8s (s : (int, int8_signed_elt, c_layout) Array1.t) so (d : (int, int8_signed_elt, c_layout) Array1.t) do_ len =
  for i = 0 to len - 1 do Array1.unsafe_set d (do_ + i) (Array1.unsafe_get s (so + i)) done
let row_i8u (s : (int, int8_unsigned_elt, c_layout) Array1.t) so (d : (int, int8_unsigned_elt, c_layout) Array1.t) do_ len =
  for i = 0 to len - 1 do Array1.unsafe_set d (do_ + i) (Array1.unsafe_get s (so + i)) done
let row_i16s (s : (int, int16_signed_elt, c_layout) Array1.t) so (d : (int, int16_signed_elt, c_layout) Array1.t) do_ len =
  for i = 0 to len - 1 do Array1.unsafe_set d (do_ + i) (Array1.unsafe_get s (so + i)) done
let row_i16u (s : (int, int16_unsigned_elt, c_layout) Array1.t) so (d : (int, int16_unsigned_elt, c_layout) Array1.t) do_ len =
  for i = 0 to len - 1 do Array1.unsafe_set d (do_ + i) (Array1.unsafe_get s (so + i)) done
let row_i32 (s : (int32, int32_elt, c_layout) Array1.t) so (d : (int32, int32_elt, c_layout) Array1.t) do_ len =
  for i = 0 to len - 1 do Array1.unsafe_set d (do_ + i) (Array1.unsafe_get s (so + i)) done
let row_i64 (s : (int64, int64_elt, c_layout) Array1.t) so (d : (int64, int64_elt, c_layout) Array1.t) do_ len =
  for i = 0 to len - 1 do Array1.unsafe_set d (do_ + i) (Array1.unsafe_get s (so + i)) done
let row_f32 (s : (float, float32_elt, c_layout) Array1.t) so (d : (float, float32_elt, c_layout) Array1.t) do_ len =
  for i = 0 to len - 1 do Array1.unsafe_set d (do_ + i) (Array1.unsafe_get s (so + i)) done
let row_f64 (s : (float, float64_elt, c_layout) Array1.t) so (d : (float, float64_elt, c_layout) Array1.t) do_ len =
  for i = 0 to len - 1 do Array1.unsafe_set d (do_ + i) (Array1.unsafe_get s (so + i)) done
let row_c32 (s : (Complex.t, complex32_elt, c_layout) Array1.t) so (d : (Complex.t, complex32_elt, c_layout) Array1.t) do_ len =
  for i = 0 to len - 1 do Array1.unsafe_set d (do_ + i) (Array1.unsafe_get s (so + i)) done
let row_c64 (s : (Complex.t, complex64_elt, c_layout) Array1.t) so (d : (Complex.t, complex64_elt, c_layout) Array1.t) do_ len =
  for i = 0 to len - 1 do Array1.unsafe_set d (do_ + i) (Array1.unsafe_get s (so + i)) done
let row_char (s : (char, int8_unsigned_elt, c_layout) Array1.t) so (d : (char, int8_unsigned_elt, c_layout) Array1.t) do_ len =
  for i = 0 to len - 1 do Array1.unsafe_set d (do_ + i) (Array1.unsafe_get s (so + i)) done

(* Rows of at least this many elements are copied with [Array1.blit] (a
   memmove); shorter ones by an element loop, which avoids allocating two
   bigarray proxies per row and is faster below roughly this length. *)
let blit_row_threshold = 128

(* Walk the region row by row, calling [copy_row src_off dst_off len].
   Offsets are in elements of the flat views. *)
let iter_rows ~src_dims ~src_offset ~dst_dims ~dst_offset ~region copy_row =
  let ndim = Array.length region in
  let sstr = strides src_dims and dstr = strides dst_dims in
  let base str off = Array.fold_left ( + ) 0 (Array.mapi (fun d o -> o * str.(d)) off) in
  let last = ndim - 1 in
  let row = region.(last) in
  let rec go d soff doff =
    if d = last then copy_row soff doff row
    else
      for i = 0 to region.(d) - 1 do
        go (d + 1) (soff + i * sstr.(d)) (doff + i * dstr.(d))
      done
  in
  go 0 (base sstr src_offset) (base dstr dst_offset)

let check_region ~src_dims ~src_offset ~dst_dims ~dst_offset ~region =
  let ndim = Array.length region in
  if Array.length src_dims <> ndim || Array.length dst_dims <> ndim
     || Array.length src_offset <> ndim || Array.length dst_offset <> ndim then
    invalid_arg "Ndarray.blit: rank mismatch";
  for d = 0 to ndim - 1 do
    if region.(d) < 0 || src_offset.(d) < 0 || dst_offset.(d) < 0
       || src_offset.(d) + region.(d) > src_dims.(d)
       || dst_offset.(d) + region.(d) > dst_dims.(d) then
      invalid_arg "Ndarray.blit: region out of bounds"
  done

(** [blit ~src ~src_offset ~dst ~dst_offset ~shape] copies the hyper-rectangle
    of extent [shape] starting at [src_offset] in [src] to [dst_offset] in
    [dst].  Both arrays must have the same kind and rank.
    Raises [Invalid_argument] on a kind, rank or bounds mismatch. *)
let blit ~src ~src_offset ~dst ~dst_offset ~shape:region =
  let src_dims = shape src and dst_dims = shape dst in
  check_region ~src_dims ~src_offset ~dst_dims ~dst_offset ~region;
  let ndim = Array.length region in
  if Array.exists (fun r -> r = 0) region then ()
  else if region = src_dims && region = dst_dims then
    apply2 { f2 = Genarray.blit } src dst
  else begin
    let rows copy_row s d =
      let s = flat s and d = flat d in
      let row = region.(ndim - 1) in
      let copy =
        if row >= blit_row_threshold then
          (fun so do_ len -> Array1.blit (Array1.sub s so len) (Array1.sub d do_ len))
        else (fun so do_ len -> copy_row s so d do_ len)
      in
      iter_rows ~src_dims ~src_offset ~dst_dims ~dst_offset ~region copy
    in
    match src, dst with
    | Int8_signed s, Int8_signed d -> rows row_i8s s d
    | Int8_unsigned s, Int8_unsigned d -> rows row_i8u s d
    | Int16_signed s, Int16_signed d -> rows row_i16s s d
    | Int16_unsigned s, Int16_unsigned d -> rows row_i16u s d
    | Int32 s, Int32 d -> rows row_i32 s d
    | Int64 s, Int64 d -> rows row_i64 s d
    | Float32 s, Float32 d -> rows row_f32 s d
    | Float64 s, Float64 d -> rows row_f64 s d
    | Complex32 s, Complex32 d -> rows row_c32 s d
    | Complex64 s, Complex64 d -> rows row_c64 s d
    | Char s, Char d -> rows row_char s d
    | _ -> invalid_arg "Ndarray.blit: arrays have different element kinds"
  end

(** [blit_strided] is {!blit} where consecutive elements of the region are
    [src_step] apart in [src] along each axis; [dst] receives them densely.
    Used for stepped slices; element-wise, so slower than {!blit}. *)
let blit_strided ~src ~src_offset ~src_step ~dst ~dst_offset ~shape:region =
  let ndim = Array.length region in
  if ndim > 0 && not (Array.exists (fun r -> r = 0) region) then begin
    let src_dims = shape src and dst_dims = shape dst in
    for d = 0 to ndim - 1 do
      if src_step.(d) < 1 then invalid_arg "Ndarray.blit_strided: step must be positive";
      let last_src = src_offset.(d) + (region.(d) - 1) * src_step.(d) in
      if src_offset.(d) < 0 || dst_offset.(d) < 0 || last_src >= src_dims.(d)
         || dst_offset.(d) + region.(d) > dst_dims.(d) then
        invalid_arg "Ndarray.blit_strided: region out of bounds"
    done;
    let gather (type a b) (s : (a, b) ga) (d : (a, b) ga) =
      let s = flat s and d = flat d in
      let sstr = strides src_dims and dstr = strides dst_dims in
      let rec go dim soff doff =
        if dim = ndim then Array1.unsafe_set d doff (Array1.unsafe_get s soff)
        else
          for i = 0 to region.(dim) - 1 do
            go (dim + 1)
              (soff + (src_offset.(dim) + i * src_step.(dim)) * sstr.(dim))
              (doff + (dst_offset.(dim) + i) * dstr.(dim))
          done
      in
      go 0 0 0
    in
    apply2 { f2 = gather } src dst
  end

(** Inverse of {!blit_strided}: scatter the dense [src] region into [dst] with
    [dst_step] between consecutive elements along each axis. *)
let scatter_strided ~src ~src_offset ~dst ~dst_offset ~dst_step ~shape:region =
  let ndim = Array.length region in
  if ndim > 0 && not (Array.exists (fun r -> r = 0) region) then begin
    let src_dims = shape src and dst_dims = shape dst in
    for d = 0 to ndim - 1 do
      if dst_step.(d) < 1 then invalid_arg "Ndarray.scatter_strided: step must be positive";
      let last_dst = dst_offset.(d) + (region.(d) - 1) * dst_step.(d) in
      if src_offset.(d) < 0 || dst_offset.(d) < 0 || last_dst >= dst_dims.(d)
         || src_offset.(d) + region.(d) > src_dims.(d) then
        invalid_arg "Ndarray.scatter_strided: region out of bounds"
    done;
    let scatter (type a b) (s : (a, b) ga) (d : (a, b) ga) =
      let s = flat s and d = flat d in
      let sstr = strides src_dims and dstr = strides dst_dims in
      let rec go dim soff doff =
        if dim = ndim then Array1.unsafe_set d doff (Array1.unsafe_get s soff)
        else
          for i = 0 to region.(dim) - 1 do
            go (dim + 1)
              (soff + (src_offset.(dim) + i) * sstr.(dim))
              (doff + (dst_offset.(dim) + i * dst_step.(dim)) * dstr.(dim))
          done
      in
      go 0 0 0
    in
    apply2 { f2 = scatter } src dst
  end

(** A fresh copy of [arr]. *)
let copy arr =
  let dst = empty (dtype arr) (shape arr) in
  apply2 { f2 = Genarray.blit } arr dst;
  dst

(** {1 Whole-array operations} *)

(** Structural equality of shape and elements ([=] on elements, so NaN is not
    equal to NaN). *)
let equal a b =
  shape a = shape b
  && (match a, b with
      | Int8_signed _, Int8_signed _ | Int8_unsigned _, Int8_unsigned _
      | Int16_signed _, Int16_signed _ | Int16_unsigned _, Int16_unsigned _
      | Int32 _, Int32 _ | Int64 _, Int64 _ | Float32 _, Float32 _
      | Float64 _, Float64 _ | Complex32 _, Complex32 _ | Complex64 _, Complex64 _
      | Char _, Char _ -> true
      | _ -> false)
  && apply2 { f2 = (fun (type a b) (x : (a, b) ga) (y : (a, b) ga) ->
      let x = flat x and y = flat y in
      let n = Array1.dim x in
      let rec go i = i >= n || (Array1.unsafe_get x i = Array1.unsafe_get y i && go (i + 1)) in
      go 0) } a b

(** Permute the axes: output axis [i] is input axis [perm.(i)]. *)
let transpose arr perm =
  let dims = shape arr in
  let ndim = Array.length dims in
  if Array.length perm <> ndim
     || List.sort compare (Array.to_list perm) <> List.init ndim Fun.id then
    invalid_arg "Ndarray.transpose: invalid permutation";
  let new_dims = Array.map (fun i -> dims.(i)) perm in
  let result = empty (dtype arr) new_dims in
  let go (type a b) (src : (a, b) ga) (dst : (a, b) ga) =
    let src = flat src and dst = flat dst in
    let n = Array1.dim src in
    if n > 0 then begin
      let sstr = strides dims in
      (* stride in the source for each output axis *)
      let out_stride = Array.map (fun i -> sstr.(i)) perm in
      let idx = Array.make ndim 0 in
      let soff = ref 0 in
      for doff = 0 to n - 1 do
        Array1.unsafe_set dst doff (Array1.unsafe_get src !soff);
        (* increment the output multi-index, odometer style *)
        let d = ref (ndim - 1) in
        let continue = ref true in
        while !continue && !d >= 0 do
          idx.(!d) <- idx.(!d) + 1;
          soff := !soff + out_stride.(!d);
          if idx.(!d) < new_dims.(!d) then continue := false
          else begin
            soff := !soff - idx.(!d) * out_stride.(!d);
            idx.(!d) <- 0;
            decr d
          end
        done
      done
    end
  in
  apply2 { f2 = go } arr result;
  result

(** Build an array from a function of the multi-index. *)
let init dtype dims f =
  let arr = create dtype dims in
  let n = numel arr in
  for i = 0 to n - 1 do
    let idx = offset_to_index dims i in
    set arr idx (f idx)
  done;
  arr

(** Build an array from a C-order list of scalars. *)
let of_list dtype dims values =
  let arr = create dtype dims in
  let n = numel arr in
  if List.length values <> n then invalid_arg "Ndarray.of_list: wrong number of elements";
  List.iteri (fun i v -> set arr (offset_to_index dims i) v) values;
  arr
