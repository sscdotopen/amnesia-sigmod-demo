extern crate ws;
extern crate serde_json;

use timely::dataflow::ProbeHandle;

use differential_dataflow::input::InputSession;
use ws::{Handler, Message, Request, Response, Result, Sender};
use differential_dataflow::operators::arrange::TraceAgent;
use differential_dataflow::trace::implementations::spine_fueled::Spine;
use differential_dataflow::trace::implementations::ord::OrdValBatch;
use timely::communication::allocator::thread::Thread;
use timely::worker::Worker;
use differential_dataflow::trace::{Cursor, TraceReader};

use std::fs::File;
use std::io::Read;

use std::cell::RefCell;
use std::rc::Rc;
use std::fmt::Debug;

use serde_json::json;
use serde_json::Result as SerdeResult;

use crate::requests::{Change, ChangeRequest};
use std::cmp::Ordering;

type Trace<K, V> = TraceAgent<Spine<K, V, usize, isize, Rc<OrdValBatch<K, V, usize, isize>>>>;
type SharedTrace<K, V> = Rc<RefCell<Trace<K, V>>>;

pub struct Server {
    pub current_step: usize,
    pub out: Sender,
    pub worker: Rc<RefCell<Worker<Thread>>>,
    pub input: Rc<RefCell<InputSession<usize, (u32, u32), isize>>>,
    pub probe: Rc<RefCell<ProbeHandle<usize>>>,
    pub shared_num_interactions_per_item_trace: SharedTrace<u32, isize>,
    pub shared_cooccurrences_trace: SharedTrace<(u32, u32), isize>,
    pub shared_similarities_trace: SharedTrace<(u32, u32), String>,
    pub shared_recommendations_trace : SharedTrace<u32, u32>,
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

    fn broadcast(&self, message: Message) {
        self.out.broadcast(message).expect("Unable to send message");
    }

    fn last_update_time(&self) -> usize {
        self.current_step - 1
    }

    fn broadcast_in_order(&self, mut changes: Vec<ChangeMessage>) {
        changes.sort();
        changes.into_iter()
            .for_each(|change| self.broadcast(change.message));
    }

    fn broadcast_num_interactions_per_item_diffs(&self) {
        let changes = collect_diffs(
            Rc::clone(&self.shared_num_interactions_per_item_trace),
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
            Rc::clone(&self.shared_cooccurrences_trace),
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
            Rc::clone(&self.shared_similarities_trace),
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
            Rc::clone(&self.shared_recommendations_trace),
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

                self.worker.borrow_mut().step_while(|| {
                    self.probe.borrow_mut().less_than(interactions_input.time())
                });

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




fn collect_diffs<K, V, F>(
    trace: SharedTrace<K, V>,
    time_of_interest: usize,
    logic: F,
) -> Vec<ChangeMessage>
    where V: Clone + Ord + Debug,
          K: Clone + Ord + Debug,
          F: Fn(&K, &V, usize, isize) -> ChangeMessage + 'static
{
    let mut messages = Vec::new();

//    let (_, storage) = trace.borrow_mut().cursor();
//
//    // We have a simple case where we only need to look at a single batch
//    trace.borrow_mut().map_batches(|batch| {
//        if batch.lower().iter().find(|t| *(*t) == time_of_interest) != None {
//            println!("{:?}", batch.description());
//
//            let mut cursor = batch.cursor();
//
//
//            while cursor.key_valid(&storage) {
//                while cursor.val_valid(&storage) {
//
//                    let key = cursor.key(&storage);
//                    let value = cursor.val(&storage);
//
//                    cursor.map_times(&storage, |time, diff| {
//                        if *time == time_of_interest {
//                            messages.push(logic(&key, &value, *time, *diff));
//                        }
//                    });
//
//                    cursor.step_val(&storage);
//                }
//                cursor.step_key(&storage);
//            }
//        }
//    });

    let (mut cursor, storage) = trace.borrow_mut().cursor();
    
    while cursor.key_valid(&storage) {
        while cursor.val_valid(&storage) {

            let key = cursor.key(&storage);
            let value = cursor.val(&storage);

            cursor.map_times(&storage, |time, diff| {
                if *time == time_of_interest {
                    messages.push(logic(&key, &value, *time, *diff));
                }
            });

            cursor.step_val(&storage);
        }
        cursor.step_key(&storage);
    }

    messages
}