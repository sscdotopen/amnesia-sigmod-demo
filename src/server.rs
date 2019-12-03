extern crate ws;
extern crate serde_json;

use timely::dataflow::ProbeHandle;

use differential_dataflow::input::InputSession;
use ws::{Handler, Message, Request, Response, Result, Sender};

use timely::communication::allocator::thread::Thread;
use timely::worker::Worker;

use std::fs::File;
use std::io::Read;

use std::cell::RefCell;
use std::sync::Arc;

use serde_json::Result as SerdeResult;

use crate::requests::{Change, ChangeRequest};

pub struct Server {
    pub current_step: usize,
    pub out: Sender,
    pub worker: Arc<RefCell<Worker<Thread>>>,
    pub input: Arc<RefCell<InputSession<usize, (u32, u32), isize>>>,
    pub probe: Arc<RefCell<ProbeHandle<usize>>>
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

                println!("{:?}", request);
            },
            Err(_) => println!("Error parsing request..."),
        }

        // Broadcast to all connections
        self.out.broadcast(Message::text("huhu"))
    }

    //
    fn on_request(&mut self, req: &Request) -> Result<(Response)> {
        match req.resource() {
            "/ws" => Response::from_request(req),
            "/" => Ok(Response::new(200, "OK", read_index_html())),
            _ => Ok(Response::new(404, "Not Found", b"404 - Not Found".to_vec())),
        }
    }
}