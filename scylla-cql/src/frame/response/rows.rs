use crate::frame::response::result::{deser_cql_value, ColumnSpec, ColumnType, Row};
use crate::frame::types;
use crate::frame::value::Timestamp;
use bytes::{Buf, Bytes};

pub trait RowDeserializer: Sized {
    fn match_column_specs(col_specs: &[ColumnSpec]) -> Result<(), ()>;
    fn deserialize(bytes: &mut Bytes, column_specs: &[ColumnSpec]) -> Result<Self, ()>;
}

impl RowDeserializer for Row {
    fn match_column_specs(_col_specs: &[ColumnSpec]) -> Result<(), ()> {
        Ok(())
    }
    fn deserialize(bytes: &mut Bytes, column_specs: &[ColumnSpec]) -> Result<Self, ()> {
        let mut chunk = bytes.chunk();
        let buf = &mut chunk;
        let mut columns = Vec::with_capacity(column_specs.len());
        for col_spec in column_specs {
            let v = if let Some(mut b) = types::read_bytes_opt(buf).map_err(|_| ())? {
                Some(deser_cql_value(&col_spec.typ, &mut b).map_err(|_| ())?)
            } else {
                None
            };
            columns.push(v);
        }
        *bytes = bytes.slice_ref(chunk);
        Ok(Row { columns })
    }
}

// should be sealed
trait RawColumnDeserializer: Sized {
    fn match_column_type(col_spec: &ColumnType) -> Result<(), ()>;
    fn deserialize_with_size(bytes: &mut Bytes, size: usize) -> Result<Self, ()>;
    fn deserialize(bytes: &mut Bytes) -> Result<Option<Self>, ()> {
        if bytes.len() < 4 {
            return Err(());
        }
        let size = bytes.get_i32();
        if size < 0 {
            Ok(None)
        } else if bytes.len() >= size as usize {
            Ok(Some(Self::deserialize_with_size(bytes, size as usize)?))
        } else {
            Err(())
        }
    }
}

impl RawColumnDeserializer for Bytes {
    fn match_column_type(col_type: &ColumnType) -> Result<(), ()> {
        match col_type {
            ColumnType::Blob => Ok(()),
            _ => Err(()),
        }
    }
    fn deserialize_with_size(bytes: &mut Bytes, size: usize) -> Result<Self, ()> {
        if bytes.len() >= size {
            Ok(bytes.split_to(size))
        } else {
            Err(())
        }
    }
}

impl RawColumnDeserializer for string::String<Bytes> {
    fn match_column_type(col_type: &ColumnType) -> Result<(), ()> {
        match col_type {
            ColumnType::Text | ColumnType::Ascii => Ok(()),
            _ => Err(()),
        }
    }
    fn deserialize_with_size(bytes: &mut Bytes, size: usize) -> Result<Self, ()> {
        string::TryFrom::try_from(Bytes::deserialize_with_size(bytes, size).map_err(|_| ())?)
            .map_err(|_| ())
    }
}

impl RawColumnDeserializer for i16 {
    fn match_column_type(col_type: &ColumnType) -> Result<(), ()> {
        match col_type {
            ColumnType::SmallInt => Ok(()),
            _ => Err(()),
        }
    }
    fn deserialize_with_size(bytes: &mut Bytes, _size: usize) -> Result<Self, ()> {
        Ok(bytes.get_i16())
    }
}

impl RawColumnDeserializer for Timestamp {
    fn match_column_type(col_type: &ColumnType) -> Result<(), ()> {
        match col_type {
            ColumnType::Timestamp => Ok(()),
            _ => Err(()),
        }
    }
    fn deserialize_with_size(bytes: &mut Bytes, _size: usize) -> Result<Self, ()> {
        Ok(Timestamp(chrono::Duration::microseconds(bytes.get_i64())))
    }
}

trait ColumnDeserializer: Sized {
    type RawDeserializer: RawColumnDeserializer;
    fn from_raw(opt: Option<Self::RawDeserializer>) -> Result<Self, ()>;
}

impl<T> ColumnDeserializer for T
where
    T: RawColumnDeserializer,
{
    type RawDeserializer = T;
    fn from_raw(opt: Option<Self::RawDeserializer>) -> Result<Self, ()> {
        opt.ok_or(())
    }
}

impl<T> ColumnDeserializer for Option<T>
where
    T: RawColumnDeserializer,
{
    type RawDeserializer = T;
    fn from_raw(opt: Option<Self::RawDeserializer>) -> Result<Self, ()> {
        Ok(opt)
    }
}

macro_rules! replace_expr {
    ($_t:tt $sub:expr) => {
        $sub
    };
}

// This macro implements FromRow for tuple of types that have FromCqlVal
macro_rules! impl_tuple_deserializers {
    ( $($Ti:tt),+ ) => {
        impl<$($Ti),+> RowDeserializer for ($($Ti,)+)
        where
            $($Ti: ColumnDeserializer),+
        {
            fn match_column_specs(column_specs: &[ColumnSpec]) -> Result<(), ()> {
                // From what I know, it is not possible yet to get the number of metavariable
                // repetitions (https://github.com/rust-lang/lang-team/issues/28#issue-644523674)
                // This is a workaround
                let expected_len = <[()]>::len(&[$(replace_expr!(($Ti) ())),*]);
                if expected_len != column_specs.len() {
                    return Err(())
                }
                let mut col_specs = column_specs.iter();
                $($Ti::RawDeserializer::match_column_type(&col_specs.next().ok_or(())?.typ)?;)+
                Ok(())
            }
            fn deserialize(bytes: &mut Bytes, _column_specs: &[ColumnSpec]) -> Result<Self, ()> {
                Ok((
                    $($Ti::from_raw($Ti::RawDeserializer::deserialize(bytes)?)?,)+
                ))
            }
        }
        impl<$($Ti),+> RawColumnDeserializer for ($($Ti,)+)
        where
            $($Ti: ColumnDeserializer),+
        {
            fn match_column_type(col_type: &ColumnType) -> Result<(), ()> {
                // From what I know, it is not possible yet to get the number of metavariable
                // repetitions (https://github.com/rust-lang/lang-team/issues/28#issue-644523674)
                // This is a workaround
                let expected_len = <[()]>::len(&[$(replace_expr!(($Ti) ())),*]);
                match col_type {
                    ColumnType::Tuple(types) => {
                        if expected_len != types.len() {
                            return Err(())
                        }
                        let mut types = types.iter();
                        $($Ti::RawDeserializer::match_column_type(types.next().ok_or(())?)?;)+
                        Ok(())
                    },
                    _ => Err(()),
                }
            }
            fn deserialize_with_size(bytes: &mut Bytes, _size: usize) -> Result<Self, ()> {
                Ok((
                    $($Ti::from_raw($Ti::RawDeserializer::deserialize(bytes)?)?,)+
                ))
            }
        }
    }
}

// Implement FromRow for tuples of size up to 16
impl_tuple_deserializers!(T1);
impl_tuple_deserializers!(T1, T2);
impl_tuple_deserializers!(T1, T2, T3);
impl_tuple_deserializers!(T1, T2, T3, T4);
impl_tuple_deserializers!(T1, T2, T3, T4, T5);
impl_tuple_deserializers!(T1, T2, T3, T4, T5, T6);
impl_tuple_deserializers!(T1, T2, T3, T4, T5, T6, T7);
impl_tuple_deserializers!(T1, T2, T3, T4, T5, T6, T7, T8);
impl_tuple_deserializers!(T1, T2, T3, T4, T5, T6, T7, T8, T9);
impl_tuple_deserializers!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10);
impl_tuple_deserializers!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11);
impl_tuple_deserializers!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12);
impl_tuple_deserializers!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13);
impl_tuple_deserializers!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14);
impl_tuple_deserializers!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15);
impl_tuple_deserializers!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16);
