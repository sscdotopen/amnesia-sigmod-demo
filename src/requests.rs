extern crate serde_derive;
extern crate serde;
extern crate serde_json;

#[derive(Eq, PartialEq, Debug, Serialize, Deserialize)]
pub enum Change {
    Add,
    Remove
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ChangeRequest {
    pub change: Change,
    pub interactions: Vec<(u32,u32)>,
}