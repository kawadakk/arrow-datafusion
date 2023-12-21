// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! File type abstraction

use crate::error::{DataFusionError, Result};

use core::fmt;
use std::fmt::Display;
use std::str::FromStr;

/// The default file extension of arrow files
pub const DEFAULT_ARROW_EXTENSION: &str = ".arrow";
/// The default file extension of avro files
pub const DEFAULT_AVRO_EXTENSION: &str = ".avro";
/// The default file extension of csv files
pub const DEFAULT_CSV_EXTENSION: &str = ".csv";
/// The default file extension of json files
pub const DEFAULT_JSON_EXTENSION: &str = ".json";
/// The default file extension of parquet files
pub const DEFAULT_PARQUET_EXTENSION: &str = ".parquet";

/// Define each `FileType`/`FileCompressionType`'s extension
pub trait GetExt {
    /// File extension getter
    fn get_ext(&self) -> String;
}

/// Readable file type
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum FileType {
    /// Apache Arrow file
    Arrow,
    /// Apache Avro file
    Avro,
    /// Apache Parquet file
    #[cfg(feature = "parquet")]
    Parquet,
    /// CSV file
    Csv,
    /// JSON file
    Json,
}

impl GetExt for FileType {
    fn get_ext(&self) -> String {
        match self {
            FileType::Arrow => DEFAULT_ARROW_EXTENSION.to_owned(),
            FileType::Avro => DEFAULT_AVRO_EXTENSION.to_owned(),
            #[cfg(feature = "parquet")]
            FileType::Parquet => DEFAULT_PARQUET_EXTENSION.to_owned(),
            FileType::Csv => DEFAULT_CSV_EXTENSION.to_owned(),
            FileType::Json => DEFAULT_JSON_EXTENSION.to_owned(),
        }
    }
}

impl Display for FileType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let out = match self {
            FileType::Csv => "csv",
            FileType::Json => "json",
            #[cfg(feature = "parquet")]
            FileType::Parquet => "parquet",
            FileType::Avro => "avro",
            FileType::Arrow => "arrow",
        };
        write!(f, "{}", out)
    }
}

impl FromStr for FileType {
    type Err = DataFusionError;

    fn from_str(s: &str) -> Result<Self> {
        let s = s.to_uppercase();
        match s.as_str() {
            "ARROW" => Ok(FileType::Arrow),
            "AVRO" => Ok(FileType::Avro),
            #[cfg(feature = "parquet")]
            "PARQUET" => Ok(FileType::Parquet),
            "CSV" => Ok(FileType::Csv),
            "JSON" | "NDJSON" => Ok(FileType::Json),
            _ => Err(DataFusionError::NotImplemented(format!(
                "Unknown FileType: {s}"
            ))),
        }
    }
}

#[cfg(test)]
#[cfg(feature = "parquet")]
mod tests {
    use crate::error::DataFusionError;
    use crate::file_options::FileType;
    use std::str::FromStr;

    #[test]
    fn from_str() {
        for (ext, file_type) in [
            ("csv", FileType::Csv),
            ("CSV", FileType::Csv),
            ("json", FileType::Json),
            ("JSON", FileType::Json),
            ("avro", FileType::Avro),
            ("AVRO", FileType::Avro),
            ("parquet", FileType::Parquet),
            ("PARQUET", FileType::Parquet),
        ] {
            assert_eq!(FileType::from_str(ext).unwrap(), file_type);
        }

        assert!(matches!(
            FileType::from_str("Unknown"),
            Err(DataFusionError::NotImplemented(_))
        ));
    }
}
