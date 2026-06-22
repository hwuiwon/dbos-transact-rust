//! Serialization of workflow inputs, outputs, events, and errors.
//!
//! Typed `P`/`R` values are erased to [`serde_json::Value`] at the typed boundary
//! and to `Option<String>` (`None` == SQL `NULL`) at the DB boundary. Each stored
//! row carries its own `serialization` tag so the right decoder is selected on
//! read.
//!
//! Framing:
//! * `DBOS_JSON` — base64 (STANDARD/padded) of the JSON bytes; nil → `__DBOS_NIL`.
//! * `portable_json` — raw JSON; nil → `null`.

use std::any::TypeId;
use std::sync::Arc;

use base64::Engine;
use serde::{Serialize, de::DeserializeOwned};

use crate::error::{DbosError, DbosErrorCode, PortableWorkflowError};

/// Marker stored for nil values under the non-portable JSON serializer.
pub const NIL_MARKER: &str = "__DBOS_NIL";
/// Serialization format name for cross-language interop.
pub const PORTABLE_SERIALIZER_NAME: &str = "portable_json";
/// Default (non-portable) JSON serialization format name.
pub const JSON_SERIALIZER_NAME: &str = "DBOS_JSON";
/// Gob serialization name (recognized on read only; emission unsupported).
pub const GOB_SERIALIZER_NAME: &str = "DBOS_GOB";

/// Errors raised by the (de)serialization layer. These are wrapped into
/// [`DbosError`] at the generic bridge.
#[derive(Debug, thiserror::Error)]
pub enum SerializeError {
    #[error("failed to encode data: {0}")]
    Encode(#[source] serde_json::Error),
    #[error("failed to decode json data: {0}")]
    DecodeJson(#[source] serde_json::Error),
    #[error("failed to decode base64 data: {0}")]
    DecodeBase64(#[source] base64::DecodeError),
    #[error("unknown serialization format {0:?}")]
    UnknownFormat(String),
    #[error("gob serialization is not supported (format {0:?})")]
    GobUnsupported(String),
}

impl From<SerializeError> for DbosError {
    fn from(e: SerializeError) -> Self {
        DbosError::new(DbosErrorCode::WorkflowUnexpectedType, e.to_string()).with_source(e)
    }
}

/// Object-safe serializer over [`serde_json::Value`]. Stored on the context as
/// `Arc<dyn Serializer>`.
pub trait Serializer: Send + Sync {
    /// The format name persisted in the row's `serialization` column.
    fn name(&self) -> &'static str;
    /// Encode a value to the stored `*string` form (`None` == SQL NULL — never
    /// returned by the built-ins, which use nil markers instead).
    fn encode_value(&self, v: &serde_json::Value) -> Result<Option<String>, SerializeError>;
    /// Decode the stored `*string` form back to a value (nil → `Value::Null`).
    fn decode_value(&self, s: &Option<String>) -> Result<serde_json::Value, SerializeError>;
}

/// Default serializer: base64 of JSON bytes, `__DBOS_NIL` for nil.
#[derive(Debug, Default, Clone, Copy)]
pub struct JsonSerializer;

impl Serializer for JsonSerializer {
    fn name(&self) -> &'static str {
        JSON_SERIALIZER_NAME
    }

    fn encode_value(&self, v: &serde_json::Value) -> Result<Option<String>, SerializeError> {
        if v.is_null() {
            return Ok(Some(NIL_MARKER.to_string()));
        }
        let bytes = serde_json::to_vec(v).map_err(SerializeError::Encode)?;
        Ok(Some(base64::engine::general_purpose::STANDARD.encode(bytes)))
    }

    fn decode_value(&self, s: &Option<String>) -> Result<serde_json::Value, SerializeError> {
        match s {
            None => Ok(serde_json::Value::Null),
            Some(s) if s == NIL_MARKER => Ok(serde_json::Value::Null),
            Some(s) => {
                let bytes = base64::engine::general_purpose::STANDARD
                    .decode(s)
                    .map_err(SerializeError::DecodeBase64)?;
                serde_json::from_slice(&bytes).map_err(SerializeError::DecodeJson)
            }
        }
    }
}

/// Cross-language serializer: raw JSON, `null` for nil.
#[derive(Debug, Default, Clone, Copy)]
pub struct PortableSerializer;

impl Serializer for PortableSerializer {
    fn name(&self) -> &'static str {
        PORTABLE_SERIALIZER_NAME
    }

    fn encode_value(&self, v: &serde_json::Value) -> Result<Option<String>, SerializeError> {
        if v.is_null() {
            return Ok(Some("null".to_string()));
        }
        let s = serde_json::to_string(v).map_err(SerializeError::Encode)?;
        Ok(Some(s))
    }

    fn decode_value(&self, s: &Option<String>) -> Result<serde_json::Value, SerializeError> {
        match s {
            None => Ok(serde_json::Value::Null),
            Some(s) if s == "null" => Ok(serde_json::Value::Null),
            Some(s) => serde_json::from_str(s).map_err(SerializeError::DecodeJson),
        }
    }
}

/// Generic typed → stored bridge (`typed -> Value -> framed *string`).
pub(crate) fn encode<R: Serialize>(
    ser: &dyn Serializer,
    v: &R,
) -> Result<Option<String>, DbosError> {
    let value = serde_json::to_value(v).map_err(SerializeError::Encode)?;
    Ok(ser.encode_value(&value)?)
}

/// Generic stored → typed bridge (`framed *string -> Value -> typed`).
pub(crate) fn decode<R: DeserializeOwned>(
    ser: &dyn Serializer,
    s: &Option<String>,
) -> Result<R, DbosError> {
    let value = ser.decode_value(s)?;
    Ok(serde_json::from_value(value).map_err(SerializeError::DecodeJson)?)
}

/// Pick the encoder for values written inside a workflow.
/// Priority: portable workflow → context custom → default JSON.
pub(crate) fn resolve_encoder(
    is_portable: bool,
    custom: Option<&Arc<dyn Serializer>>,
) -> Arc<dyn Serializer> {
    if is_portable {
        return Arc::new(PortableSerializer);
    }
    if let Some(c) = custom {
        return c.clone();
    }
    Arc::new(JsonSerializer)
}

/// Pick the decoder for a stored value based on its `serialization` tag.
/// Priority: `portable_json` → context custom (name match) → default JSON.
pub(crate) fn resolve_decoder(
    stored: &str,
    custom: Option<&Arc<dyn Serializer>>,
) -> Result<Arc<dyn Serializer>, SerializeError> {
    if stored == PORTABLE_SERIALIZER_NAME {
        return Ok(Arc::new(PortableSerializer));
    }
    if let Some(c) = custom {
        if c.name() == stored {
            return Ok(c.clone());
        }
    }
    if stored.is_empty() || stored == JSON_SERIALIZER_NAME {
        return Ok(Arc::new(JsonSerializer));
    }
    if stored == GOB_SERIALIZER_NAME {
        return Err(SerializeError::GobUnsupported(stored.to_string()));
    }
    Err(SerializeError::UnknownFormat(stored.to_string()))
}

// --- portable workflow-input envelope ---

/// Cross-language envelope for workflow inputs.
#[derive(Debug, Clone, Default, Serialize, serde::Deserialize)]
pub struct PortableWorkflowArgs {
    #[serde(rename = "positionalArgs", default)]
    pub positional_args: Vec<serde_json::Value>,
    #[serde(rename = "namedArgs", default)]
    pub named_args: std::collections::HashMap<String, serde_json::Value>,
}

/// Wrap a value into the portable args envelope and encode it as plain JSON.
/// A value that is already [`PortableWorkflowArgs`] is encoded as-is; otherwise
/// it becomes the single positional arg of a fresh envelope.
pub(crate) fn encode_portable_args<P: Serialize + 'static>(
    data: &P,
) -> Result<Option<String>, DbosError> {
    let value = if TypeId::of::<P>() == TypeId::of::<PortableWorkflowArgs>() {
        serde_json::to_value(data).map_err(SerializeError::Encode)?
    } else {
        let arg = serde_json::to_value(data).map_err(SerializeError::Encode)?;
        serde_json::json!({ "positionalArgs": [arg], "namedArgs": {} })
    };
    Ok(PortableSerializer.encode_value(&value)?)
}

/// Unwrap the first positional arg from a portable envelope into `T`. If `T` is
/// the envelope type itself, the full envelope is returned.
pub(crate) fn decode_portable_args<T: DeserializeOwned + 'static>(
    data: &Option<String>,
) -> Result<T, DbosError> {
    let raw = match data {
        None => return Ok(serde_json::from_value(serde_json::Value::Null).map_err(SerializeError::DecodeJson)?),
        Some(s) if s == "null" => {
            return Ok(serde_json::from_value(serde_json::Value::Null).map_err(SerializeError::DecodeJson)?);
        }
        Some(s) => s,
    };
    if TypeId::of::<T>() == TypeId::of::<PortableWorkflowArgs>() {
        return Ok(serde_json::from_str(raw).map_err(SerializeError::DecodeJson)?);
    }
    let envelope: PortableWorkflowArgs =
        serde_json::from_str(raw).map_err(SerializeError::DecodeJson)?;
    match envelope.positional_args.into_iter().next() {
        None => Ok(serde_json::from_value(serde_json::Value::Null).map_err(SerializeError::DecodeJson)?),
        Some(v) => Ok(serde_json::from_value(v).map_err(SerializeError::DecodeJson)?),
    }
}

// --- error (de)serialization ---

/// Serialize an error for DB storage. Portable workflows use the JSON envelope;
/// all others store the plain message string.
pub(crate) fn serialize_workflow_error(err: &DbosError, serialization: &str) -> String {
    if serialization != PORTABLE_SERIALIZER_NAME {
        return err.to_string();
    }
    // If the source is itself a PortableWorkflowError, reuse it.
    let err_data = portable_from_source(err).unwrap_or_else(|| PortableWorkflowError {
        name: "Portable Error".to_string(),
        message: err.to_string(),
        code: None,
        data: None,
    });
    serde_json::to_string(&err_data).unwrap_or_else(|_| err.to_string())
}

fn portable_from_source(err: &DbosError) -> Option<PortableWorkflowError> {
    let mut src = std::error::Error::source(err);
    while let Some(s) = src {
        if let Some(pe) = s.downcast_ref::<PortableWorkflowError>() {
            return Some(pe.clone());
        }
        src = s.source();
    }
    None
}

/// Deserialize an error from DB storage. Portable serialization parses the JSON
/// envelope; all others build a plain-message [`DbosError`].
pub(crate) fn deserialize_workflow_error(
    err_str: &Option<String>,
    serialization: &str,
) -> Option<DbosError> {
    let s = match err_str {
        None => return None,
        Some(s) if s.is_empty() => return None,
        Some(s) => s,
    };
    if serialization != PORTABLE_SERIALIZER_NAME {
        return Some(plain_error(s));
    }
    match serde_json::from_str::<PortableWorkflowError>(s) {
        Ok(pe) => {
            let mut e = DbosError::new(DbosErrorCode::WorkflowExecution, pe.message.clone());
            e.source = Some(Arc::new(pe));
            Some(e)
        }
        Err(_) => Some(plain_error(s)),
    }
}

/// Build a plain `DbosError` carrying a recovered message string. Recovered
/// errors are categorized as workflow-execution errors.
fn plain_error(message: &str) -> DbosError {
    DbosError::new(DbosErrorCode::WorkflowExecution, message.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_roundtrip_and_nil_marker() {
        let ser = JsonSerializer;
        // value round-trip
        let enc = encode(&ser, &vec![1, 2, 3]).unwrap();
        let back: Vec<i32> = decode(&ser, &enc).unwrap();
        assert_eq!(back, vec![1, 2, 3]);
        // nil marker for None
        let enc_nil = encode::<Option<i32>>(&ser, &None).unwrap();
        assert_eq!(enc_nil.as_deref(), Some(NIL_MARKER));
        let back_nil: Option<i32> = decode(&ser, &enc_nil).unwrap();
        assert_eq!(back_nil, None);
        // None column decodes to null
        let back_null: Option<i32> = decode(&ser, &None).unwrap();
        assert_eq!(back_null, None);
        // empty-but-present collection is NOT nil
        let enc_empty = encode::<Vec<i32>>(&ser, &vec![]).unwrap();
        assert_ne!(enc_empty.as_deref(), Some(NIL_MARKER));
        let back_empty: Vec<i32> = decode(&ser, &enc_empty).unwrap();
        assert_eq!(back_empty, Vec::<i32>::new());
    }

    #[test]
    fn portable_raw_json_and_null() {
        let ser = PortableSerializer;
        let enc = encode(&ser, &"hello").unwrap();
        assert_eq!(enc.as_deref(), Some("\"hello\""));
        let back: String = decode(&ser, &enc).unwrap();
        assert_eq!(back, "hello");
        let enc_nil = encode::<Option<i32>>(&ser, &None).unwrap();
        assert_eq!(enc_nil.as_deref(), Some("null"));
    }

    #[test]
    fn resolve_decoder_precedence() {
        assert_eq!(resolve_decoder("", None).unwrap().name(), JSON_SERIALIZER_NAME);
        assert_eq!(resolve_decoder("DBOS_JSON", None).unwrap().name(), JSON_SERIALIZER_NAME);
        assert_eq!(
            resolve_decoder("portable_json", None).unwrap().name(),
            PORTABLE_SERIALIZER_NAME
        );
        assert!(resolve_decoder("DBOS_GOB", None).is_err());
        assert!(resolve_decoder("nonsense", None).is_err());
    }

    #[test]
    fn portable_args_envelope() {
        let enc = encode_portable_args(&42i32).unwrap();
        // wrapped as a positional arg
        assert_eq!(enc.as_deref(), Some("{\"namedArgs\":{},\"positionalArgs\":[42]}"));
        let back: i32 = decode_portable_args(&enc).unwrap();
        assert_eq!(back, 42);
    }
}
