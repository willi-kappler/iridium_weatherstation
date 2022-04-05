
use serde_derive::Deserialize;

#[derive(Deserialize, Debug)]
pub struct IWConfiguration {
    pub ports: Vec<u16>,
    pub alive_message_intervall: u64,
}
