extern crate ws;
extern crate serde_json;

use std::fs::File;
use std::io::Read;
use std::cell::RefCell;
use std::rc::Rc;
use std::fmt::Debug;
use std::cmp::Ordering;

use timely::dataflow::ProbeHandle;
use timely::communication::allocator::thread::Thread;
use timely::worker::Worker;

use differential_dataflow::input::InputSession;
use differential_dataflow::trace::{Cursor, TraceReader};

use ws::{Handler, Message, Request, Response, Result, Sender};

use serde_json::json;
use serde_json::Result as SerdeResult;

use crate::requests::{Change, ChangeRequest};
use crate::types::Trace;


pub struct Server {
    current_step: usize,
    out: Sender,
    worker: Worker<Thread>,
    input: Rc<RefCell<InputSession<usize, (u32, u32), isize>>>,
    probe: ProbeHandle<usize>,
    num_interactions_per_item_trace: Trace<u32, isize>,
    cooccurrences_trace: Trace<(u32, u32), isize>,
    similarities_trace: Trace<(u32, u32), String>,
    recommendations_trace : Trace<u32, u32>,
}

fn read_local(file: &str) -> Vec<u8> {
    let mut data = Vec::new();

    let mut file = File::open(file).expect("Unable to read file!");
    file.read_to_end(&mut data).expect("Unable to read file!");

    data
}

#[derive(Eq, PartialEq)]
struct ChangeMessage {
    pub change: isize,
    pub message: Message,
}

impl ChangeMessage {
    pub fn new(change: isize, message: Message) -> Self {
        ChangeMessage { change, message }
    }
}

impl PartialOrd for ChangeMessage {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.change.cmp(&other.change))
    }
}

impl Ord for ChangeMessage {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(&other).unwrap()
    }
}

impl Server {

    pub fn new(
        current_step: usize,
        out: Sender,
        worker: Worker<Thread>,
        input: Rc<RefCell<InputSession<usize, (u32, u32), isize>>>,
        probe: ProbeHandle<usize>,
        num_interactions_per_item_trace: Trace<u32, isize>,
        cooccurrences_trace: Trace<(u32, u32), isize>,
        similarities_trace: Trace<(u32, u32), String>,
        recommendations_trace : Trace<u32, u32>,
    ) -> Self {

        Server {
            current_step,
            out,
            worker,
            input,
            probe,
            num_interactions_per_item_trace,
            cooccurrences_trace,
            similarities_trace,
            recommendations_trace
        }
    }

    fn broadcast(&self, message: Message) {
        self.out.broadcast(message).expect("Unable to send message");
    }

    fn last_update_time(&self) -> usize {
        self.current_step - 1
    }

    fn broadcast_in_order(&self, mut changes: Vec<ChangeMessage>) {
        changes.sort();
        changes.into_iter()
            .for_each(|change| {
                println!("\t{}", change.message.as_text().unwrap());
                self.broadcast(change.message)
            });
    }

    fn broadcast_num_interactions_per_item_diffs(&self) {
        let changes = collect_diffs(
            self.num_interactions_per_item_trace.clone(),
            self.last_update_time(),
            |item, count, time, change| {

                let json = json!({
                            "data": "item_interactions_n",
                            "item": item,
                            "count": count,
                            "time": time,
                            "change": change
                        });

                ChangeMessage::new(change, Message::text(json.to_string()))
            });

        self.broadcast_in_order(changes);
    }

    fn broadcast_cooccurrences_diffs(&self) {
        let changes = collect_diffs(
            self.cooccurrences_trace.clone(),
            self.last_update_time(),
            |(item_a, item_b), num_cooccurrences, time, change| {

                let json = json!({
                            "data": "cooccurrences_c",
                            "item_a": item_a,
                            "item_b": item_b,
                            "num_cooccurrences": num_cooccurrences,
                            "time": time,
                            "change": change
                        });

                ChangeMessage::new(change, Message::text(json.to_string()))
            });

        self.broadcast_in_order(changes);
    }

    fn broadcast_similarities_diffs(&self) {
        let changes = collect_diffs(
            self.similarities_trace.clone(),
            self.last_update_time(),
            |(item_a, item_b), similarity, time, change| {

                let json = json!({
                            "data": "similarities_s",
                            "item_a": item_a,
                            "item_b": item_b,
                            "similarity": similarity.parse::<f64>().unwrap(),
                            "time": time,
                            "change": change
                        });

                ChangeMessage::new(change, Message::text(json.to_string()))
            });

        self.broadcast_in_order(changes);
    }

    fn broadcast_recommendation_diffs(&self) {
        let changes = collect_diffs(
            self.recommendations_trace.clone(),
            self.last_update_time(),
            |query, item, time, change| {

                let json = json!({
                            "data": "recommendations",
                            "query": query,
                            "item": item,
                            "time": time,
                            "change": change
                        });

                ChangeMessage::new(change, Message::text(json.to_string()))
            });

        self.broadcast_in_order(changes);
    }
}


impl Handler for Server {

    fn on_message(&mut self, msg: Message) -> Result<()> {

        // We assume we always get valid utf-8
        let message_as_string = &msg.into_text().unwrap();

        let parsed_request: SerdeResult<ChangeRequest> =
            serde_json::from_slice(&message_as_string.as_bytes());

        match parsed_request {
            Ok(request) => {

                println!("Received request: {:?}", request);

                self.current_step += 1;

                let mut interactions_input = self.input.borrow_mut();

                if request.change == Change::Add {
                    for (user, item) in request.interactions.iter() {
                        interactions_input.insert((*user, *item));
                    }
                } else {
                    for (user, item) in request.interactions.iter() {
                        interactions_input.remove((*user, *item));
                    }
                }

                interactions_input.advance_to(self.current_step);
                interactions_input.flush();

                let worker = &mut self.worker;
                let probe = &self.probe;

                worker.step_while(|| probe.less_than(interactions_input.time()));

                self.broadcast_num_interactions_per_item_diffs();
                self.broadcast_cooccurrences_diffs();
                self.broadcast_similarities_diffs();
                self.broadcast_recommendation_diffs();
            },
            Err(e) => println!("Error parsing request:\n{:?}\n\n{:?}\n", &message_as_string, e),
        }

        Ok(())
    }

    fn on_request(&mut self, req: &Request) -> Result<(Response)> {
        match req.resource() {
            "/ws" => Response::from_request(req),
            "/style.css" => Ok(Response::new(200, "OK", read_local("html/style.css"))),
            "/script.js" => Ok(Response::new(200, "OK", read_local("html/script.js"))),
            "/" => Ok(Response::new(200, "OK", read_local("html/index.html"))),
            _ => Ok(Response::new(404, "Not Found", b"404 - Not Found".to_vec())),
        }
    }
}

use differential_dataflow::trace::BatchReader;

fn collect_diffs<K, V, F>(
    mut trace: Trace<K, V>,
    time_of_interest: usize,
    logic: F,
) -> Vec<ChangeMessage>
    where V: Clone + Ord + Debug,
          K: Clone + Ord + Debug,
          F: Fn(&K, &V, usize, isize) -> ChangeMessage + 'static
{
    let mut messages = Vec::new();

    trace.map_batches(|batch| {
        if batch.lower().iter().find(|t| *(*t) == time_of_interest) != None {

            let mut cursor = batch.cursor();

            while cursor.key_valid(&batch) {
                while cursor.val_valid(&batch) {

                    let key = cursor.key(&batch);
                    let value = cursor.val(&batch);

                    cursor.map_times(&batch, |time, diff| {
                        if *time == time_of_interest {
                            messages.push(logic(&key, &value, *time, *diff));
                        }
                    });

                    cursor.step_val(&batch);
                }
                cursor.step_key(&batch);
            }
        }
    });

    trace.distinguish_since(&[]);
    trace.advance_by(&[time_of_interest]);

    messages
}
