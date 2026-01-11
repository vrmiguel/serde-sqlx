use crate::databases::Database;
use crate::map_access::RowMapAccess;
use serde::de::{value::Error as DeError, Deserializer, Visitor};
use serde::de::{Error as _, IntoDeserializer};
use serde::forward_to_deserialize_any;
use sqlx::{ColumnIndex, Row, TypeInfo, ValueRef};

pub struct RowDeserializer<'a, DB: Database> {
    pub(crate) row: &'a <DB as sqlx::Database>::Row,
    pub(crate) index: usize,
}

impl<'a, DB: Database> RowDeserializer<'a, DB> {
    pub fn new(row: &'a <DB as sqlx::Database>::Row) -> Self {
        RowDeserializer { row, index: 0 }
    }
}

impl<'de, 'a, DB: Database> Deserializer<'de> for RowDeserializer<'a, DB>
where
    usize: ColumnIndex<<DB as sqlx::Database>::Row>,
{
    type Error = DeError;

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let raw_value = self.row.try_get_raw(0).map_err(DeError::custom)?;

        if raw_value.is_null() {
            visitor.visit_none()
        } else {
            visitor.visit_some(self)
        }
    }

    fn deserialize_newtype_struct<V>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.row.columns().len() {
            0 => return visitor.visit_unit(),
            1 => {}
            _n => {
                return self.deserialize_seq(visitor);
            }
        };

        let raw_value = self.row.try_get_raw(self.index).map_err(DeError::custom)?;
        let type_info = raw_value.type_info();
        let type_name = type_info.name();

        if raw_value.is_null() {
            return visitor.visit_none();
        }

        // If this is a BOOL[], TEXT[], etc
        if DB::is_sequence(type_name) {
            return self.deserialize_seq(visitor);
        }

        // Direct all "basic" types down to `ValueDeserializer`
        let deserializer = ValueDeserializer::<'_, DB>::new(raw_value);
        deserializer.deserialize_any(visitor)
    }

    /// We treat the row as a map (each column is a key/value pair)
    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let num_cols = self.row.columns().len();

        visitor.visit_map(RowMapAccess {
            deserializer: self,
            num_cols,
        })
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        DB::deserialize_seq(visitor, self)
    }

    fn deserialize_tuple<V>(self, _len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_seq(visitor)
    }

    fn deserialize_struct<V>(
        self,
        _name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let raw_value = self.row.try_get_raw(self.index).map_err(DeError::custom)?;

        if let Some(json) = DB::deserialize_json(raw_value)? {
            if let serde_json::Value::Object(ref obj) = json {
                if fields.len() == 1 {
                    // If there's only one expected field, check if the object already contains it.
                    if obj.contains_key(fields[0]) {
                        // If so, we can deserialize directly.
                        return json
                            .into_deserializer()
                            .deserialize_any(visitor)
                            .map_err(DeError::custom);
                    } else {
                        // Otherwise, wrap the object in a new map keyed by that field name.
                        let mut map = serde_json::Map::new();
                        map.insert(fields[0].to_owned(), json);
                        return map
                            .into_deserializer()
                            .deserialize_any(visitor)
                            .map_err(DeError::custom);
                    }
                } else {
                    // For multiple expected fields, ensure the JSON object already contains all of them.
                    if fields.iter().all(|&field| obj.contains_key(field)) {
                        return json
                            .into_deserializer()
                            .deserialize_any(visitor)
                            .map_err(DeError::custom);
                    } else {
                        return Err(DeError::custom(format!(
                            "JSON object missing expected keys: expected {:?}, found keys {:?}",
                            fields,
                            obj.keys().collect::<Vec<_>>()
                        )));
                    }
                }
            } else {
                // For non-object JSON values, delegate directly.
                return json
                    .into_deserializer()
                    .deserialize_any(visitor)
                    .map_err(DeError::custom);
            }
        };

        // Fallback for non-JSON types.
        self.deserialize_map(visitor)
    }

    fn deserialize_enum<V>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let raw_value = self.row.try_get_raw(self.index).map_err(DeError::custom)?;

        if raw_value.is_null() {
            return visitor.visit_none();
        }

        // Direct all "basic" types down to `ValueDeserializer`
        let mut deserializer = ValueDeserializer::<'_, DB>::new(raw_value);
        deserializer.value_type = ValueType::Enum;
        deserializer.deserialize_any(visitor)
    }

    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let raw_value = self.row.try_get_raw(self.index).map_err(DeError::custom)?;
        if raw_value.is_null() {
            return visitor.visit_none();
        }

        let mut deserializer = ValueDeserializer::<'_, DB>::new(raw_value);
        deserializer.value_type = ValueType::Bool;
        deserializer.deserialize_any(visitor)
    }

    // For other types, forward to deserialize_any.
    forward_to_deserialize_any! {
        i8 i16 i32 i64 u8 u16 u32 u64 f32 f64 char str string
        bytes byte_buf unit unit_struct
        tuple_struct identifier ignored_any
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum ValueType {
    Any,
    Enum,
    Bool,
}

/// An "inner" deserializer
pub struct ValueDeserializer<'a, DB: Database> {
    pub(crate) value: <DB as sqlx::Database>::ValueRef<'a>,
    pub(crate) value_type: ValueType,
}

impl<'a, DB: Database> ValueDeserializer<'a, DB> {
    pub fn new(val: <DB as sqlx::Database>::ValueRef<'a>) -> Self {
        Self {
            value: val,
            value_type: ValueType::Any,
        }
    }
}

impl<'de, 'a, DB: Database> Deserializer<'de> for ValueDeserializer<'a, DB> {
    type Error = DeError;

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        if self.value.is_null() {
            visitor.visit_none()
        } else {
            visitor.visit_some(self)
        }
    }

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        if self.value.is_null() {
            return visitor.visit_none();
        }
        DB::deserialize_value(self, visitor)
    }

    fn deserialize_enum<V>(
        mut self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.value_type = ValueType::Enum;
        self.deserialize_any(visitor)
    }

    fn deserialize_bool<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.value_type = ValueType::Bool;
        self.deserialize_any(visitor)
    }

    // For other types, forward to deserialize_any.
    forward_to_deserialize_any! {
        i8 i16 i32 i64 u8 u16 u32 u64 f32 f64 char str string
        bytes byte_buf unit unit_struct newtype_struct struct
        tuple_struct identifier ignored_any tuple seq map
    }
}
