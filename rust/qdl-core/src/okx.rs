use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OkxService {
    Public,
    Business,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct OkxSubscription {
    pub channel: String,
    #[serde(rename = "instId")]
    pub inst_id: String,
}

impl OkxSubscription {
    pub fn service(&self) -> Result<OkxService, String> {
        match self.channel.as_str() {
            "trades" | "bbo-tbt" | "books" => Ok(OkxService::Public),
            "candle1m" | "trades-all" => Ok(OkxService::Business),
            _ => Err(format!("unsupported stable OKX channel: {}", self.channel)),
        }
    }

    pub fn provider_kind(&self) -> Result<&'static str, String> {
        match self.channel.as_str() {
            "trades" | "trades-all" => Ok("okx_trade"),
            "bbo-tbt" => Ok("okx_bbo"),
            "candle1m" => Ok("okx_bar"),
            "books" => Ok("okx_book"),
            _ => Err(format!("unsupported stable OKX channel: {}", self.channel)),
        }
    }

    fn validate(&self) -> Result<(), String> {
        if self.inst_id.trim().is_empty() {
            return Err("OKX subscription instrument is required".into());
        }
        self.service().map(|_| ())
    }
}

#[derive(Serialize)]
struct SubscribeCommand<'a> {
    id: &'a str,
    op: &'static str,
    args: &'a [OkxSubscription],
}

pub fn subscription_command(id: &str, subscriptions: &[OkxSubscription]) -> Result<String, String> {
    if id.trim().is_empty() || id.len() > 32 || subscriptions.is_empty() {
        return Err("OKX subscription id/list is invalid".into());
    }
    let mut service = None;
    for subscription in subscriptions {
        subscription.validate()?;
        let current = subscription.service()?;
        if service.is_some_and(|expected| expected != current) {
            return Err("OKX public and business subscriptions require separate sockets".into());
        }
        service = Some(current);
    }
    serde_json::to_string(&SubscribeCommand {
        id,
        op: "subscribe",
        args: subscriptions,
    })
    .map_err(|error| error.to_string())
}

pub fn parse_subscription_ack(
    payload: &Value,
    expected_id: &str,
    pending: &mut Vec<(String, String)>,
) -> Result<bool, String> {
    match payload.get("event").and_then(Value::as_str) {
        Some("error") => {
            return Err(format!(
                "OKX subscription rejected code={} message={}",
                payload
                    .get("code")
                    .and_then(Value::as_str)
                    .unwrap_or("missing"),
                payload
                    .get("msg")
                    .and_then(Value::as_str)
                    .unwrap_or("missing")
            ))
        }
        Some("subscribe") => {}
        Some(other) => return Err(format!("unexpected OKX control event: {other}")),
        None => return Ok(false),
    }
    if payload.get("id").and_then(Value::as_str) != Some(expected_id) {
        return Err("OKX subscription ACK id mismatch".into());
    }
    let argument = payload
        .get("arg")
        .and_then(Value::as_object)
        .ok_or_else(|| "OKX subscription ACK arg is missing".to_owned())?;
    let channel = argument
        .get("channel")
        .and_then(Value::as_str)
        .ok_or_else(|| "OKX subscription ACK channel is missing".to_owned())?;
    let inst_id = argument
        .get("instId")
        .and_then(Value::as_str)
        .ok_or_else(|| "OKX subscription ACK instrument is missing".to_owned())?;
    let index = pending
        .iter()
        .position(|item| item.0 == channel && item.1 == inst_id)
        .ok_or_else(|| "OKX subscription ACK is duplicate or undeclared".to_owned())?;
    pending.remove(index);
    Ok(true)
}

#[derive(Clone, Debug)]
pub struct NativeFrame {
    pub provider_kind: &'static str,
    pub instrument_id: String,
    pub raw: Value,
}

pub fn expand_data_frame(payload: &Value) -> Result<Vec<NativeFrame>, String> {
    if payload.get("event").is_some() {
        return Ok(vec![]);
    }
    let argument = payload
        .get("arg")
        .and_then(Value::as_object)
        .ok_or_else(|| "OKX data frame arg must be an object".to_owned())?;
    let subscription = OkxSubscription {
        channel: argument
            .get("channel")
            .and_then(Value::as_str)
            .ok_or_else(|| "OKX data frame channel is missing".to_owned())?
            .to_owned(),
        inst_id: argument
            .get("instId")
            .and_then(Value::as_str)
            .ok_or_else(|| "OKX data frame instrument is missing".to_owned())?
            .to_owned(),
    };
    subscription.validate()?;
    let rows = payload
        .get("data")
        .and_then(Value::as_array)
        .filter(|rows| !rows.is_empty())
        .ok_or_else(|| "OKX data frame rows are missing".to_owned())?;
    match subscription.channel.as_str() {
        "trades" | "trades-all" => rows
            .iter()
            .map(|row| {
                let object = row
                    .as_object()
                    .ok_or_else(|| "OKX trade row must be an object".to_owned())?;
                if object.get("instId").and_then(Value::as_str)
                    != Some(subscription.inst_id.as_str())
                {
                    return Err("OKX trade row instrument mismatch".into());
                }
                Ok(NativeFrame {
                    provider_kind: "okx_trade",
                    instrument_id: subscription.inst_id.clone(),
                    raw: row.clone(),
                })
            })
            .collect(),
        "bbo-tbt" | "candle1m" | "books" => {
            if rows.len() != 1 {
                return Err("OKX BBO/BAR/BOOK frame requires one data row".into());
            }
            Ok(vec![NativeFrame {
                provider_kind: subscription.provider_kind()?,
                instrument_id: subscription.inst_id,
                raw: payload.clone(),
            }])
        }
        _ => Err("unsupported OKX stable frame".into()),
    }
}

#[derive(Clone, Debug, Default)]
pub struct ControlRequestBudget {
    window_started_ns: i64,
    used: u16,
}

impl ControlRequestBudget {
    const WINDOW_NS: i64 = 3_600_000_000_000;
    const MAX_REQUESTS: u16 = 480;

    pub fn permit(&mut self, now_ns: i64) -> Result<(), String> {
        if now_ns <= 0 {
            return Err("OKX control request clock is invalid".into());
        }
        if self.window_started_ns == 0
            || now_ns.saturating_sub(self.window_started_ns) >= Self::WINDOW_NS
        {
            self.window_started_ns = now_ns;
            self.used = 0;
        }
        if self.used >= Self::MAX_REQUESTS {
            return Err("OKX control request budget exhausted".into());
        }
        self.used += 1;
        Ok(())
    }

    pub fn used(&self) -> u16 {
        self.used
    }
}

#[cfg(test)]
mod tests {
    use super::{
        expand_data_frame, parse_subscription_ack, subscription_command, ControlRequestBudget,
        OkxService, OkxSubscription,
    };
    use serde_json::json;

    fn subscription(channel: &str) -> OkxSubscription {
        OkxSubscription {
            channel: channel.into(),
            inst_id: "BTC-USDT-SWAP".into(),
        }
    }

    #[test]
    fn public_and_business_commands_are_separate_and_ack_correlated() {
        let command =
            subscription_command("7", &[subscription("trades"), subscription("bbo-tbt")]).unwrap();
        assert!(command.contains(r#""op":"subscribe""#));
        assert!(
            subscription_command("8", &[subscription("trades"), subscription("candle1m")]).is_err()
        );
        assert_eq!(
            subscription("trades").service().unwrap(),
            OkxService::Public
        );
        assert_eq!(
            subscription("candle1m").service().unwrap(),
            OkxService::Business
        );

        let mut pending = vec![
            ("trades".into(), "BTC-USDT-SWAP".into()),
            ("bbo-tbt".into(), "BTC-USDT-SWAP".into()),
        ];
        assert!(parse_subscription_ack(
            &json!({"id":"7","event":"subscribe","arg":{
                "channel":"trades","instId":"BTC-USDT-SWAP"
            }}),
            "7",
            &mut pending
        )
        .unwrap());
        assert_eq!(pending.len(), 1);
        assert!(parse_subscription_ack(
            &json!({"id":"8","event":"subscribe","arg":{
                "channel":"bbo-tbt","instId":"BTC-USDT-SWAP"
            }}),
            "7",
            &mut pending
        )
        .is_err());
    }

    #[test]
    fn trade_rows_expand_atomically_but_bbo_and_bar_keep_native_frame() {
        let trades = expand_data_frame(&json!({
            "arg":{"channel":"trades","instId":"BTC-USDT-SWAP"},
            "data":[
                {"instId":"BTC-USDT-SWAP","tradeId":"1"},
                {"instId":"BTC-USDT-SWAP","tradeId":"2"}
            ]
        }))
        .unwrap();
        assert_eq!(trades.len(), 2);
        assert!(trades.iter().all(|item| item.provider_kind == "okx_trade"));

        for (channel, kind, row) in [
            ("bbo-tbt", "okx_bbo", json!({"seqId":1,"bids":[],"asks":[]})),
            (
                "candle1m",
                "okx_bar",
                json!(["1", "1", "1", "1", "1", "1", "1", "1", "0"]),
            ),
        ] {
            let items = expand_data_frame(&json!({
                "arg":{"channel":channel,"instId":"BTC-USDT-SWAP"},
                "data":[row]
            }))
            .unwrap();
            assert_eq!(items.len(), 1);
            assert_eq!(items[0].provider_kind, kind);
            assert_eq!(items[0].raw["arg"]["channel"], channel);
        }
    }

    #[test]
    fn control_budget_is_bounded_and_resets_after_one_hour() {
        let mut budget = ControlRequestBudget::default();
        for _ in 0..480 {
            budget.permit(1).unwrap();
        }
        assert_eq!(budget.used(), 480);
        assert!(budget.permit(2).is_err());
        budget.permit(3_600_000_000_001).unwrap();
        assert_eq!(budget.used(), 1);
    }
}
