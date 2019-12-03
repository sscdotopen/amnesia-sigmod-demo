extern crate timely;
extern crate ws;
extern crate amnesia_sigmod_demo;

use std::cell::RefCell;
use std::sync::Arc;

use timely::communication::allocator::thread::Thread;
use timely::worker::Worker;

use differential_dataflow::input::InputSession;
use differential_dataflow::operators::CountTotal;

use ws::listen;

use amnesia_sigmod_demo::server::Server;


fn main() {
    let alloc = Thread::new();
    let worker = Arc::new(RefCell::new(timely::worker::Worker::new(alloc)));

    demo(Arc::clone(&worker));

    while worker.borrow_mut().step_or_park(None) { }

}

fn demo(worker: Arc<RefCell<Worker<Thread>>>) {

    let mut interactions_input: InputSession<usize, (u32, u32), isize> = InputSession::new();

    let the_probe = worker.borrow_mut().dataflow(|scope| {
        let interactions = interactions_input.to_collection(scope);

        interactions
            .map(|(_user, item)| item)
            .count_total()
            .inspect(|x| println!("{:?}", x))
            .probe()
    });

    let input = Arc::new(RefCell::new(interactions_input));
    let probe = Arc::new(RefCell::new(the_probe));

    // Listen on an address and call the closure for each connection
    listen("127.0.0.1:8000", |out| {
        Server {
            current_step: 0,
            out,
            worker: Arc::clone(&worker),
            input: Arc::clone(&input),
            probe: Arc::clone(&probe)
        }
    }).unwrap();
}
