use super::Transform;
use crate::{
    config::{log_schema, DataType, TransformConfig, TransformDescription},
    event::{Event, PathComponent, PathIter},
    internal_events::{
        GrokParserConversionFailed, GrokParserEventProcessed, GrokParserFailedMatch,
        GrokParserMissingField,
    },
    types::{parse_conversion_map, Conversion},
};
use grok::Pattern;
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu};
use std::collections::HashMap;
use std::str;

#[derive(Debug, Snafu)]
enum BuildError {
    #[snafu(display("Invalid grok pattern: {}", source))]
    InvalidGrok { source: grok::Error },
}

#[derive(Deserialize, Serialize, Debug, Derivative)]
#[serde(deny_unknown_fields, default)]
#[derivative(Default)]
pub struct GrokParserConfig {
    pub pattern: String,
    pub field: Option<String>,
    #[derivative(Default(value = "true"))]
    pub drop_field: bool,
    pub types: HashMap<String, String>,
}

inventory::submit! {
    TransformDescription::new::<GrokParserConfig>("grok_parser")
}

impl_generate_config_from_default!(GrokParserConfig);

#[async_trait::async_trait]
#[typetag::serde(name = "grok_parser")]
impl TransformConfig for GrokParserConfig {
    async fn build(&self) -> crate::Result<Box<dyn Transform>> {
        let field = self
            .field
            .clone()
            .unwrap_or_else(|| log_schema().message_key().into());

        let mut grok = grok::Grok::with_patterns();

        let types = parse_conversion_map(&self.types)?;

        Ok(grok
            .compile(&self.pattern, true)
            .map::<Box<dyn Transform>, _>(|p| {
                Box::new(GrokParser {
                    pattern: p,
                    field: field.clone(),
                    drop_field: self.drop_field,
                    types,
                    paths: HashMap::new(),
                })
            })
            .context(InvalidGrok)?)
    }

    fn input_type(&self) -> DataType {
        DataType::Log
    }

    fn output_type(&self) -> DataType {
        DataType::Log
    }

    fn transform_type(&self) -> &'static str {
        "grok_parser"
    }
}

pub struct GrokParser {
    pattern: Pattern,
    field: String,
    drop_field: bool,
    types: HashMap<String, Conversion>,
    paths: HashMap<String, Vec<PathComponent>>,
}

impl Transform for GrokParser {
    fn transform(&mut self, event: Event) -> Option<Event> {
        let mut event = event.into_log();
        let value = event.get(&self.field).map(|s| s.to_string_lossy());
        emit!(GrokParserEventProcessed);

        if let Some(value) = value {
            if let Some(matches) = self.pattern.match_against(&value) {
                let drop_field = self.drop_field && matches.get(&self.field).is_none();
                for (name, value) in matches.iter() {
                    let conv = self.types.get(name).unwrap_or(&Conversion::Bytes);
                    match conv.convert(value.to_string().into()) {
                        Ok(value) => {
                            if let Some(path) = self.paths.get(name) {
                                event.insert_path(path.to_vec(), value.clone());
                            } else {
                                let path = PathIter::new(name).collect::<Vec<_>>();
                                self.paths.insert(name.to_string(), path.clone());
                                event.insert_path(path, value);
                            }
                        }
                        Err(error) => emit!(GrokParserConversionFailed { name, error }),
                    }
                }

                if drop_field {
                    event.remove(&self.field);
                }
            } else {
                emit!(GrokParserFailedMatch {
                    value: value.as_ref()
                });
            }
        } else {
            emit!(GrokParserMissingField {
                field: self.field.as_ref()
            });
        }

        Some(Event::Log(event))
    }
}

#[cfg(test)]
mod tests {
    use super::GrokParserConfig;
    use crate::event::LogEvent;
    use crate::{
        config::{log_schema, TransformConfig},
        event, Event,
    };
    use pretty_assertions::assert_eq;
    use serde_json::json;

    #[test]
    fn generate_config() {
        crate::test_util::test_generate_config::<GrokParserConfig>();
    }

    async fn parse_log(
        event: &str,
        pattern: &str,
        field: Option<&str>,
        drop_field: bool,
        types: &[(&str, &str)],
    ) -> LogEvent {
        let event = Event::from(event);
        let mut parser = GrokParserConfig {
            pattern: pattern.into(),
            field: field.map(|s| s.into()),
            drop_field,
            types: types.iter().map(|&(k, v)| (k.into(), v.into())).collect(),
        }
        .build()
        .await
        .unwrap();
        parser.transform(event).unwrap().into_log()
    }

    #[tokio::test]
    async fn grok_parser_adds_parsed_fields_to_event() {
        let event = parse_log(
            r#"109.184.11.34 - - [12/Dec/2015:18:32:56 +0100] "GET /administrator/ HTTP/1.1" 200 4263"#,
            "%{HTTPD_COMMONLOG}",
            None,
            true,
            &[],
        ).await;

        let expected = json!({
            "clientip": "109.184.11.34",
            "ident": "-",
            "auth": "-",
            "timestamp": "12/Dec/2015:18:32:56 +0100",
            "verb": "GET",
            "request": "/administrator/",
            "httpversion": "1.1",
            "rawrequest": "",
            "response": "200",
            "bytes": "4263",
        });

        assert_eq!(expected, serde_json::to_value(&event.all_fields()).unwrap());
    }

    #[tokio::test]
    async fn grok_parser_does_nothing_on_no_match() {
        let event = parse_log(
            r#"Help I'm stuck in an HTTP server"#,
            "%{HTTPD_COMMONLOG}",
            None,
            true,
            &[],
        )
        .await;

        assert_eq!(2, event.keys().count());
        assert_eq!(
            event::Value::from("Help I'm stuck in an HTTP server"),
            event[log_schema().message_key()]
        );
        assert!(!event[log_schema().timestamp_key()]
            .to_string_lossy()
            .is_empty());
    }

    #[tokio::test]
    async fn grok_parser_can_not_drop_parsed_field() {
        let event = parse_log(
            r#"109.184.11.34 - - [12/Dec/2015:18:32:56 +0100] "GET /administrator/ HTTP/1.1" 200 4263"#,
            "%{HTTPD_COMMONLOG}",
            None,
            false,
            &[],
        ).await;

        let expected = json!({
            "clientip": "109.184.11.34",
            "ident": "-",
            "auth": "-",
            "timestamp": "12/Dec/2015:18:32:56 +0100",
            "verb": "GET",
            "request": "/administrator/",
            "httpversion": "1.1",
            "rawrequest": "",
            "response": "200",
            "bytes": "4263",
            "message": r#"109.184.11.34 - - [12/Dec/2015:18:32:56 +0100] "GET /administrator/ HTTP/1.1" 200 4263"#,
        });

        assert_eq!(expected, serde_json::to_value(&event.all_fields()).unwrap());
    }

    #[tokio::test]
    async fn grok_parser_does_nothing_on_missing_field() {
        let event = parse_log(
            "i am the only field",
            "^(?<foo>.*)",
            Some("bar"),
            false,
            &[],
        )
        .await;

        assert_eq!(2, event.keys().count());
        assert_eq!(
            event::Value::from("i am the only field"),
            event[log_schema().message_key()]
        );
        assert!(!event[log_schema().timestamp_key()]
            .to_string_lossy()
            .is_empty());
    }

    #[tokio::test]
    async fn grok_parser_coerces_types() {
        let event = parse_log(
            r#"109.184.11.34 - - [12/Dec/2015:18:32:56 +0100] "GET /administrator/ HTTP/1.1" 200 4263"#,
            "%{HTTPD_COMMONLOG}",
            None,
            true,
            &[("response", "int"), ("bytes", "int")],
        ).await;

        let expected = json!({
            "clientip": "109.184.11.34",
            "ident": "-",
            "auth": "-",
            "timestamp": "12/Dec/2015:18:32:56 +0100",
            "verb": "GET",
            "request": "/administrator/",
            "httpversion": "1.1",
            "rawrequest": "",
            "response": 200,
            "bytes": 4263,
        });

        assert_eq!(expected, serde_json::to_value(&event.all_fields()).unwrap());
    }

    #[tokio::test]
    async fn grok_parser_does_not_drop_parsed_message_field() {
        let event = parse_log(
            "12/Dec/2015:18:32:56 +0100 42",
            "%{HTTPDATE:timestamp} %{NUMBER:message}",
            None,
            true,
            &[],
        )
        .await;

        let expected = json!({
            "timestamp": "12/Dec/2015:18:32:56 +0100",
            "message": "42",
        });

        assert_eq!(expected, serde_json::to_value(&event.all_fields()).unwrap());
    }
}
