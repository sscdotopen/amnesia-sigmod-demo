extern crate timely;
extern crate ws;
extern crate amnesia_sigmod_demo;

use std::cell::RefCell;
use std::rc::Rc;

use timely::communication::allocator::thread::Thread;
use timely::worker::Worker;

use ws::listen;

use amnesia_sigmod_demo::server::Server;
use amnesia_sigmod_demo::recommender::dataflow_for_recommender;

fn main() {
    let alloc = Thread::new();
    let mut worker = timely::worker::Worker::new(alloc);

    demo(worker.clone());

    while worker.step_or_park(None) { }
}

fn demo(worker: Worker<Thread>) {

    let (interactions_input, mut query_input, probe, num_interactions_per_item_trace,
        cooccurrences_trace, similarities_trace, recommendations_trace) =
        dataflow_for_recommender(worker.clone());


    // Edward Scissorhands + Titanic
    query_input.insert((40000, 1));
    query_input.insert((40000, 4));

    // The Godfather
    query_input.insert((50000, 0));

    // Leon the Professional
    query_input.insert((60000, 2));

    query_input.advance_to(1);
    query_input.flush();
    query_input.close();


    let input = Rc::new(RefCell::new(interactions_input));

    listen("127.0.0.1:8000", |out| {
        Server::new(
            0,
            out,
            worker.clone(),
            Rc::clone(&input),
            probe.clone(),
            num_interactions_per_item_trace.clone(),
            cooccurrences_trace.clone(),
            similarities_trace.clone(),
            recommendations_trace.clone(),
        )
    }).unwrap();
}
