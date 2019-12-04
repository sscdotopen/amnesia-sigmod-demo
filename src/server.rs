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
use std::sync::Arc;
use std::rc::Rc;

use serde_json::json;
use serde_json::Result as SerdeResult;

use crate::requests::{Change, ChangeRequest};

pub struct Server {
    pub current_step: usize,
    pub out: Sender,
    pub worker: Arc<RefCell<Worker<Thread>>>,
    pub input: Arc<RefCell<InputSession<usize, (u32, u32), isize>>>,
    pub probe: Arc<RefCell<ProbeHandle<usize>>>,
    pub shared_num_interactions_per_item_trace: Arc<RefCell<TraceAgent<Spine<u32, isize, usize, isize, Rc<OrdValBatch<u32, isize, usize, isize>>>>>>,
    pub shared_cooccurrences_trace: Arc<RefCell<TraceAgent<Spine<(u32, u32), isize, usize, isize, Rc<OrdValBatch<(u32, u32), isize, usize, isize>>>>>>,
    pub shared_similarities_trace: Arc<RefCell<TraceAgent<Spine<(u32, u32), String, usize, isize, Rc<OrdValBatch<(u32, u32), String, usize, isize>>>>>>,
}

fn read_index_html() -> Vec<u8> {
    let mut data = Vec::new();

    let mut file = File::open("html/index.html").expect("Unable to read file!");
    file.read_to_end(&mut data).expect("Unable to read file!");

    data
}


impl Handler for Server {
    // Handle messages received in the websocket (in this case, only on /ws)
    fn on_message(&mut self, msg: Message) -> Result<()> {

        let parsed_request: SerdeResult<ChangeRequest> = serde_json::from_slice(&msg.into_data());

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

                //TODO collect changes from traces...

                println!("{:?}", request);

                //let (mut cursor, storage) = self.trace.borrow_mut().cursor();

                let num_interactions_per_item_messages = collect_diffs(
                    Arc::clone(&self.shared_num_interactions_per_item_trace),
                    self.current_step - 1,
                    |key, value, time, change| {

                        let json = json!({
                            "data": "item_interactions_n",
                            "item": key,
                            "count": value,
                            "time": time,
                            "change": change
                        });

                        Message::text(json.to_string())
                    });


                for message in num_interactions_per_item_messages.iter() {
                    // TODO remove clone maybe with drain or so
                    self.out.broadcast(message.clone()).expect("Cannot send message");
                }

                let cooccurrence_messages = collect_diffs(
                    Arc::clone(&self.shared_cooccurrences_trace),
                    self.current_step - 1,
                    |key, value, time, change| {

                        let (item_a, item_b) = key;

                        let json = json!({
                            "data": "cooccurrences_c",
                            "item_a": item_a,
                            "item_b": item_b,
                            "num_cooccurrences": value,
                            "time": time,
                            "change": change
                        });

                        Message::text(json.to_string())
                    });


                for message in cooccurrence_messages.iter() {
                    // TODO remove clone maybe with drain or so
                    self.out.broadcast(message.clone()).expect("Cannot send message");
                }

                let similarities_messages = collect_diffs(
                    Arc::clone(&self.shared_similarities_trace),
                    self.current_step - 1,
                    |key, value, time, change| {

                        let (item_a, item_b) = key;

                        let json = json!({
                            "data": "similarities_s",
                            "item_a": item_a,
                            "item_b": item_b,
                            "similarity": value.parse::<f64>().unwrap(),
                            "time": time,
                            "change": change
                        });

                        Message::text(json.to_string())
                    });


                for message in similarities_messages.iter() {
                    // TODO remove clone maybe with drain or so
                    self.out.broadcast(message.clone()).expect("Cannot send message");
                }

            },
            Err(_) => println!("Error parsing request..."),
        }

        Ok(())
    }

    fn on_request(&mut self, req: &Request) -> Result<(Response)> {
        match req.resource() {
            "/ws" => Response::from_request(req),
            "/" => Ok(Response::new(200, "OK", read_index_html())),
            _ => Ok(Response::new(404, "Not Found", b"404 - Not Found".to_vec())),
        }
    }
}

use std::fmt::Debug;


fn collect_diffs<K, V, F>(
    trace: Arc<RefCell<TraceAgent<Spine<K, V, usize, isize, Rc<OrdValBatch<K, V, usize, isize>>>>>>,
    time_of_interest: usize,
    logic: F,
) -> Vec<Message>
    where V: Clone + Ord + Debug,
          K: Clone + Ord + Debug,
          F: Fn(&K, &V, usize, isize) -> Message +'static
{
    // TODO dont like it that we have to buffer them
    let mut messages = Vec::new();

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