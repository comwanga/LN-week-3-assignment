use bitcoincore_rpc::{
    bitcoin::hashes::{sha256, Hash},
    Auth, Client as BitcoinClient, RpcApi,
};
use reqwest::blocking::Client;
use serde_json::{json, Value};
use std::{
    error::Error,
    fs,
    path::Path,
    str::FromStr,
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

type Result<T> = std::result::Result<T, Box<dyn Error>>;
const PAYMENT_MSAT: u64 = 100_000_000;
const WAIT_TIMEOUT: Duration = Duration::from_secs(120);

/// The provided test runner creates each node's REST authentication rune.
struct LightningNode {
    name: &'static str,
    url: String,
    rune: String,
    client: Client,
}

impl LightningNode {
    fn new(name: &'static str, port: u16, rune_variable: &str) -> Result<Self> {
        let rune = std::env::var(rune_variable).map_err(|_| {
            format!("Set {rune_variable} to {name}'s createrune result before running")
        })?;
        if rune.trim().is_empty() {
            return Err(format!("{rune_variable} must not be empty").into());
        }
        Ok(Self {
            name,
            url: format!("http://localhost:{port}/v1"),
            rune,
            client: Client::builder()
                .connect_timeout(Duration::from_secs(3))
                .timeout(Duration::from_secs(90))
                .build()?,
        })
    }

    fn call(&self, method: &str, params: Value) -> Result<Value> {
        let response = self
            .client
            .post(format!("{}/{method}", self.url))
            .header("Rune", &self.rune)
            .json(&params)
            .send()?;
        let status = response.status();
        let body: Value = response.json()?;
        if !status.is_success() || body.get("error").is_some() || body.get("code").is_some() {
            return Err(format!("{} {method} failed (HTTP {status}): {body}", self.name).into());
        }
        Ok(body)
    }
}

fn field<'a>(value: &'a Value, key: &str) -> Result<&'a str> {
    value[key]
        .as_str()
        .ok_or_else(|| format!("Missing string field {key}: {value}").into())
}

fn array<'a>(value: &'a Value, key: &str) -> Result<&'a [Value]> {
    value[key]
        .as_array()
        .map(Vec::as_slice)
        .ok_or_else(|| format!("Missing array field {key}: {value}").into())
}

/// CLN encodes millisatoshis as either JSON integers or strings ending in msat.
fn msat(value: &Value) -> Result<u64> {
    if let Some(amount) = value.as_u64() {
        return Ok(amount);
    }
    if let Some(amount) = value.as_str() {
        return Ok(amount.strip_suffix("msat").unwrap_or(amount).parse()?);
    }
    Err(format!("Invalid millisatoshi amount: {value}").into())
}

fn require(condition: bool, message: &str) -> Result<()> {
    if condition {
        Ok(())
    } else {
        Err(message.into())
    }
}

/// Wait for asynchronous block processing, channel locking, gossip, or settlement.
fn wait_for(description: &str, mut ready: impl FnMut() -> Result<bool>) -> Result<()> {
    let start = Instant::now();
    loop {
        let detail = match ready() {
            Ok(true) => return Ok(()),
            Ok(false) => "condition not yet satisfied".to_string(),
            Err(error) => error.to_string(),
        };
        if start.elapsed() >= WAIT_TIMEOUT {
            return Err(format!("Timed out waiting for {description}: {detail}").into());
        }
        thread::sleep(Duration::from_secs(1));
    }
}

/// Block/channel waits can exceed Core's HTTP keep-alive timeout. Start each
/// Bitcoin operation with a fresh connection rather than reusing an idle socket.
fn bitcoin_rpc(wallet: Option<&str>) -> Result<BitcoinClient> {
    let url = match wallet {
        Some(name) => format!("http://localhost:18443/wallet/{name}"),
        None => "http://localhost:18443".to_string(),
    };
    Ok(BitcoinClient::new(
        &url,
        Auth::UserPass("alice".into(), "password".into()),
    )?)
}

fn mine(address: &str, blocks: u64) -> Result<()> {
    bitcoin_rpc(None)?.call::<Value>("generatetoaddress", &[json!(blocks), json!(address)])?;
    Ok(())
}

fn wait_synced(nodes: &[&LightningNode]) -> Result<()> {
    let height = bitcoin_rpc(None)?.get_block_count()?;
    wait_for("Lightning nodes to process mined blocks", || {
        for node in nodes {
            let info = node.call("getinfo", json!({}))?;
            if info["blockheight"].as_u64().unwrap_or(0) < height {
                return Ok(false);
            }
        }
        Ok(true)
    })
}

fn confirmed_funds(node: &LightningNode) -> Result<u64> {
    let funds = node.call("listfunds", json!({}))?;
    array(&funds, "outputs")?
        .iter()
        .filter(|output| output["status"] == "confirmed" && output["reserved"] != true)
        .try_fold(0, |total, output| Ok(total + msat(&output["amount_msat"])?))
}

fn channel_balance(node: &LightningNode) -> Result<u64> {
    let channels = node.call("listpeerchannels", json!({}))?;
    array(&channels, "channels")?
        .iter()
        .filter(|channel| channel["state"] == "CHANNELD_NORMAL")
        .try_fold(
            0,
            |total, channel| Ok(total + msat(&channel["to_us_msat"])?),
        )
}

fn normal_channel(node: &LightningNode, peer_id: &str) -> Result<bool> {
    let channels = node.call("listpeerchannels", json!({}))?;
    Ok(array(&channels, "channels")?
        .iter()
        .any(|channel| channel["peer_id"] == peer_id && channel["state"] == "CHANNELD_NORMAL"))
}

/// In CLN v23.11 listforwards omits payment_hash. Join its incoming channel
/// and HTLC ID to listhtlcs; HTLC IDs alone are only unique within a channel.
fn forwarded_payment(
    forwards: &Value,
    htlcs: &Value,
    payment_hash: &str,
) -> Result<Option<(u64, String)>> {
    for forward in array(forwards, "forwards")? {
        if forward["status"] != "settled" {
            continue;
        }
        for htlc in array(htlcs, "htlcs")? {
            if htlc["direction"] == "in"
                && htlc["payment_hash"] == payment_hash
                && forward["in_channel"].as_str().is_some()
                && forward["in_htlc_id"].as_u64().is_some()
                && htlc["short_channel_id"] == forward["in_channel"]
                && htlc["id"] == forward["in_htlc_id"]
            {
                let fee = msat(&forward["fee_msat"])?;
                require(
                    msat(&forward["out_msat"])? == PAYMENT_MSAT,
                    "Bob forwarded an unexpected payment amount",
                )?;
                require(
                    msat(&forward["in_msat"])? == PAYMENT_MSAT + fee,
                    "Bob's forwarding amounts do not match his fee",
                )?;
                return Ok(Some((fee, field(htlc, "payment_hash")?.to_string())));
            }
        }
    }
    Ok(None)
}

fn main() -> Result<()> {
    let alice = LightningNode::new("Alice", 3010, "ALICE_RUNE")?;
    let bob = LightningNode::new("Bob", 3011, "BOB_RUNE")?;
    let carol = LightningNode::new("Carol", 3012, "CAROL_RUNE")?;
    let nodes = [&alice, &bob, &carol];

    // 1. Wait for RPC startup, then create (or load) the dedicated mining wallet.
    println!("Waiting for Bitcoin Core and the three Lightning nodes...");
    wait_for("Bitcoin RPC", || {
        Ok(bitcoin_rpc(None)?.get_blockchain_info().is_ok())
    })?;
    let loaded: Vec<String> = bitcoin_rpc(None)?.call("listwallets", &[])?;
    if !loaded.iter().any(|name| name == "mining_wallet") {
        let wallets: Value = bitcoin_rpc(None)?.call("listwalletdir", &[])?;
        let exists = array(&wallets, "wallets")?
            .iter()
            .any(|wallet| wallet["name"] == "mining_wallet");
        bitcoin_rpc(None)?.call::<Value>(
            if exists { "loadwallet" } else { "createwallet" },
            &[json!("mining_wallet")],
        )?;
    }
    let mining_address: String = bitcoin_rpc(Some("mining_wallet"))?.call("getnewaddress", &[])?;
    // 101 blocks mature the first coinbase so it can fund both Lightning wallets.
    mine(&mining_address, 101)?;
    for node in nodes {
        wait_for(node.name, || Ok(node.call("getinfo", json!({})).is_ok()))?;
    }
    wait_synced(&nodes)?;

    // 2. Give Alice and Bob 2,000,000 sats each, leaving room for channel fees.
    println!("Funding Alice and Bob's on-chain wallets...");
    for node in [&alice, &bob] {
        let address = node.call("newaddr", json!({"addresstype": "bech32"}))?;
        bitcoin_rpc(Some("mining_wallet"))?.call::<Value>(
            "sendtoaddress",
            &[json!(field(&address, "bech32")?), json!(0.02)],
        )?;
    }
    mine(&mining_address, 6)?;
    wait_synced(&nodes)?;
    for node in [&alice, &bob] {
        wait_for(&format!("{}'s confirmed on-chain funds", node.name), || {
            Ok(confirmed_funds(node)? >= 2_000_000_000)
        })?;
    }

    // 3. Establish only Alice -> Bob -> Carol, using Docker's internal DNS/ports.
    let alice_id = field(&alice.call("getinfo", json!({}))?, "id")?.to_string();
    let bob_id = field(&bob.call("getinfo", json!({}))?, "id")?.to_string();
    let carol_id = field(&carol.call("getinfo", json!({}))?, "id")?.to_string();
    alice.call(
        "connect",
        json!({"id": bob_id, "host": "bob", "port": 9735}),
    )?;
    bob.call(
        "connect",
        json!({"id": carol_id, "host": "carol", "port": 9735}),
    )?;
    println!("Opening the 500,000 sat and 300,000 sat channels...");
    alice.call("fundchannel", json!({"id": bob_id, "amount": 500_000}))?;
    bob.call("fundchannel", json!({"id": carol_id, "amount": 300_000}))?;
    mine(&mining_address, 6)?;
    wait_synced(&nodes)?;
    wait_for(
        "both ends of both channels to reach CHANNELD_NORMAL",
        || {
            Ok(normal_channel(&alice, &bob_id)?
                && normal_channel(&bob, &alice_id)?
                && normal_channel(&bob, &carol_id)?
                && normal_channel(&carol, &bob_id)?)
        },
    )?;

    // Channel readiness precedes public gossip; wait until Alice sees both hops.
    wait_for("Alice's route through Bob to Carol", || {
        let route = alice.call(
            "getroute",
            json!({"id": carol_id, "amount_msat": PAYMENT_MSAT, "riskfactor": 1}),
        )?;
        let hops = array(&route, "route")?;
        Ok(hops.len() == 2 && hops[0]["id"] == bob_id && hops[1]["id"] == carol_id)
    })?;
    let before = [
        channel_balance(&alice)?,
        channel_balance(&bob)?,
        channel_balance(&carol)?,
    ];

    // 4. Carol invoices 100,000 sats (100,000,000 msat), and Alice pays it.
    let label = format!(
        "multihop_{}",
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
    );
    let invoice = carol.call(
        "invoice",
        json!({
            "amount_msat": PAYMENT_MSAT, "label": label, "description": "Multi-Hop Payment"
        }),
    )?;
    let bolt11 = field(&invoice, "bolt11")?.to_string();
    let payment_hash = field(&invoice, "payment_hash")?.to_string();
    println!("Paying Carol's 100,000 sat invoice through Bob...");
    let payment = alice.call("pay", json!({"bolt11": bolt11, "retry_for": 60}))?;
    require(
        payment["status"] == "complete",
        "Alice's payment did not complete",
    )?;
    require(
        payment["payment_hash"] == payment_hash,
        "Payment hash differs from Carol's invoice",
    )?;
    require(
        msat(&payment["amount_msat"])? == PAYMENT_MSAT,
        "Alice paid the wrong amount",
    )?;
    let preimage = field(&payment, "payment_preimage")?.to_string();
    let preimage_bytes = sha256::Hash::from_str(&preimage)?.to_byte_array();
    require(
        sha256::Hash::hash(&preimage_bytes).to_string() == payment_hash,
        "SHA256(preimage) does not match the invoice payment hash",
    )?;

    // 5. Independently verify the payer, recipient, and Bob's settled forward.
    wait_for("Alice's completed payment and Carol's paid invoice", || {
        let pays = alice.call("listpays", json!({"payment_hash": payment_hash}))?;
        let invoices = carol.call("listinvoices", json!({"label": label}))?;
        Ok(array(&pays, "pays")?
            .iter()
            .any(|pay| pay["payment_hash"] == payment_hash && pay["status"] == "complete")
            && array(&invoices, "invoices")?.iter().any(|invoice| {
                invoice["payment_hash"] == payment_hash
                    && invoice["status"] == "paid"
                    && msat(&invoice["amount_received_msat"]).ok() == Some(PAYMENT_MSAT)
            }))
    })?;
    let mut receipt = None;
    wait_for("Bob's settled forwarding record", || {
        receipt = forwarded_payment(
            &bob.call("listforwards", json!({}))?,
            &bob.call("listhtlcs", json!({}))?,
            &payment_hash,
        )?;
        Ok(receipt.is_some())
    })?;
    let (fee_msat, forwarded_hash) = receipt.ok_or("Missing settled forwarding record")?;
    let amount_sent = msat(&payment["amount_sent_msat"])?;
    require(
        amount_sent == PAYMENT_MSAT + fee_msat,
        "Alice's payment fee differs from Bob's forwarding fee",
    )?;

    // Alice loses amount + fee, Carol gains amount, and Bob gains just the fee:
    // his incoming channel gains more than his outgoing channel spends.
    wait_for("settled channel balances", || {
        Ok(
            channel_balance(&alice)?.checked_add(amount_sent) == Some(before[0])
                && before[1].checked_add(fee_msat) == Some(channel_balance(&bob)?)
                && before[2].checked_add(PAYMENT_MSAT) == Some(channel_balance(&carol)?),
        )
    })?;

    // 6. Write exactly seven unlabelled lines in the repository root, even when
    // invoked by cargo from the rust directory. Never manufacture payment proof.
    let output = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .ok_or("Missing repository root")?
        .join("out.txt");
    fs::write(&output, format!("{payment_hash}\n{preimage}\n{bolt11}\n{alice_id}\n{carol_id}\n{fee_msat}\n{forwarded_hash}\n"))?;
    println!(
        "Payment verified; Bob earned {fee_msat} msat. Report: {}",
        output.display()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_cln_millisatoshi_encodings() {
        for value in [json!(1000), json!("1000"), json!("1000msat")] {
            assert_eq!(msat(&value).unwrap(), 1000);
        }
        for value in [
            Value::Null,
            json!(-1),
            json!(1.5),
            json!("1sat"),
            json!("-1msat"),
        ] {
            assert!(msat(&value).is_err());
        }
    }

    fn records() -> (Value, Value) {
        (
            json!({"forwards": [{"status": "settled", "in_channel": "110x1x0", "in_htlc_id": 0,
            "in_msat": 100001001, "out_msat": 100000000, "fee_msat": "1001msat"}]}),
            json!({"htlcs": [{"direction": "in", "short_channel_id": "110x1x0", "id": 0, "payment_hash": "target"}]}),
        )
    }

    #[test]
    fn recovers_hash_from_the_matching_forward_htlc() {
        let (forwards, htlcs) = records();
        assert_eq!(
            forwarded_payment(&forwards, &htlcs, "target").unwrap(),
            Some((1001, "target".into()))
        );
        assert!(forwarded_payment(&forwards, &htlcs, "another-payment")
            .unwrap()
            .is_none());
    }

    #[test]
    fn rejects_unrelated_or_unsettled_forwarding_records() {
        for (key, value) in [
            ("short_channel_id", json!("111x1x0")),
            ("id", json!(1)),
            ("direction", json!("out")),
        ] {
            let (forwards, mut htlcs) = records();
            htlcs["htlcs"][0][key] = value;
            assert!(forwarded_payment(&forwards, &htlcs, "target")
                .unwrap()
                .is_none());
        }
        let (mut forwards, htlcs) = records();
        forwards["forwards"][0]["status"] = json!("offered");
        assert!(forwarded_payment(&forwards, &htlcs, "target")
            .unwrap()
            .is_none());
    }

    #[test]
    fn rejects_inconsistent_forwarding_fee() {
        let (mut forwards, htlcs) = records();
        forwards["forwards"][0]["fee_msat"] = json!(1002);
        assert!(forwarded_payment(&forwards, &htlcs, "target").is_err());
    }
}
