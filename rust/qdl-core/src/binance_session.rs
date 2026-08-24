use serde::{Deserialize, Serialize};

use crate::binance::validate_stream;

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct SubscriptionCommand<'a> {
    method: &'a str,
    params: &'a [String],
    id: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct SubscriptionReply {
    pub id: u64,
    pub result: Option<serde_json::Value>,
    pub code: Option<i64>,
    pub msg: Option<String>,
}

pub fn subscription_command(
    id: u64,
    subscribe: bool,
    streams: &[String],
) -> Result<String, String> {
    if id == 0 || streams.is_empty() {
        return Err("Binance command requires positive id and streams".into());
    }
    for stream in streams {
        validate_stream(stream)?;
    }
    serde_json::to_string(&SubscriptionCommand {
        method: if subscribe {
            "SUBSCRIBE"
        } else {
            "UNSUBSCRIBE"
        },
        params: streams,
        id,
    })
    .map_err(|error| error.to_string())
}

/// Returns `true` only for the requested control reply. A provider data frame
/// may race the ACK on a full-duplex socket; the caller must retain it until
/// the subscription is confirmed instead of decoding it as a malformed ACK.
pub fn parse_subscription_reply(frame: &str, expected_id: u64) -> Result<bool, String> {
    let raw: serde_json::Value = serde_json::from_str(frame).map_err(|error| error.to_string())?;
    if raw.get("id").is_none() {
        return Ok(false);
    }
    let value: SubscriptionReply =
        serde_json::from_value(raw.clone()).map_err(|error| error.to_string())?;
    if value.id != expected_id {
        return Err("Binance subscription ACK id mismatch".into());
    }
    if let Some(code) = value.code {
        return Err(format!(
            "Binance subscription rejected code={code} message={}",
            value.msg.unwrap_or_else(|| "missing".into())
        ));
    }
    if raw.get("result") != Some(&serde_json::Value::Null) {
        return Err("Binance subscription ACK has unexpected result".into());
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::{parse_subscription_reply, subscription_command};

    #[test]
    fn subscribe_and_unsubscribe_commands_are_tracked_by_id() {
        let streams = vec!["btcusdt@trade".to_owned(), "ethusdt@trade".to_owned()];
        let command = subscription_command(7, true, &streams).unwrap();
        assert_eq!(
            command,
            r#"{"method":"SUBSCRIBE","params":["btcusdt@trade","ethusdt@trade"],"id":7}"#
        );
        assert!(parse_subscription_reply(r#"{"result":null,"id":7}"#, 7).unwrap());
        assert!(!parse_subscription_reply(
            r#"{"e":"trade","s":"BTCUSDT","t":1,"p":"1","q":"1","T":1,"m":false}"#,
            7,
        )
        .unwrap());
        assert!(parse_subscription_reply(r#"{"result":null,"id":8}"#, 7).is_err());
    }

    #[test]
    fn provider_rejection_fails_closed() {
        let error = parse_subscription_reply(r#"{"id":7,"code":2,"msg":"Invalid request"}"#, 7)
            .unwrap_err();
        assert!(error.contains("rejected"));
    }
}
