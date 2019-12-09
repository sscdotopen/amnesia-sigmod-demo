extern crate timely;
extern crate ws;
extern crate amnesia_sigmod_demo;

use std::cell::RefCell;
use std::rc::Rc;

use timely::communication::allocator::thread::Thread;
use timely::worker::Worker;

use differential_dataflow::input::InputSession;

use ws::listen;

use amnesia_sigmod_demo::server::Server;
use differential_dataflow::operators::arrange::ArrangeByKey;
use timely::dataflow::operators::Probe;
use differential_dataflow::operators::join::JoinCore;
use differential_dataflow::operators::{CountTotal, Count, Consolidate};

fn main() {
    let alloc = Thread::new();
    let worker = Rc::new(RefCell::new(timely::worker::Worker::new(alloc)));

    demo(Rc::clone(&worker));

    while worker.borrow_mut().step_or_park(None) { }
}

fn demo(worker: Rc<RefCell<Worker<Thread>>>) {

    let mut interactions_input: InputSession<usize, (u32, u32), isize> = InputSession::new();

    let mut the_probe = timely::dataflow::operators::probe::Handle::new();

    let (num_interactions_per_item_trace, cooccurrences_trace,
        jaccard_similarities_trace)= worker.borrow_mut().dataflow(|scope| {

        let interactions = interactions_input.to_collection(scope);

        let num_interactions_per_item = interactions
            .map(|(_user, item)| item)
            .count_total()
            .consolidate();

        let arranged_remaining_interactions = interactions.arrange_by_key();

        // Compute the number of cooccurrences of each item pair
        let cooccurrences = arranged_remaining_interactions
            .join_core(&arranged_remaining_interactions, |_user, &item_a, &item_b| {
                if item_a > item_b { Some((item_a, item_b)) } else { None }
            })
            .count()
            .consolidate();

        let arranged_cooccurrences = cooccurrences.arrange_by_key();

        let arranged_num_interactions_per_item = num_interactions_per_item.arrange_by_key();

        // Compute the jaccard similarity between item pairs (= number of users that
        // interacted with both items / number of users that interacted with at least
        // one of the items)
        let jaccard_similarities = cooccurrences
            // Find the number of interactions for item_a
            .map(|((item_a, item_b), num_cooc)| (item_a, (item_b, num_cooc)))
            .join_core(
                &arranged_num_interactions_per_item,
                |&item_a, &(item_b, num_cooc), &occ_a| {
                    Some((item_b, (item_a, num_cooc, occ_a)))
                }
            )
            // Find the number of interactions for item_b
            .join_core(
                &arranged_num_interactions_per_item,
                |&item_b, &(item_a, num_cooc, occ_a), &occ_b| {
                    Some(((item_a, item_b), (num_cooc, occ_a, occ_b)))
                },
            )
            // Compute Jaccard similarty, has to be done in a map due to the lack of a
            // total order for f64 (which seems to break the consolidation in join)
            .map(|((item_a, item_b), (num_cooc, occ_a, occ_b))| {
                let jaccard = num_cooc as f64 / (occ_a + occ_b - num_cooc) as f64;
                ((item_a, item_b), jaccard.to_string())
            })
            .consolidate();

        let arranged_jaccard_similarities = jaccard_similarities.arrange_by_key();

        arranged_jaccard_similarities.stream.probe_with(&mut the_probe);

        (arranged_num_interactions_per_item.trace, arranged_cooccurrences.trace,
            arranged_jaccard_similarities.trace)
    });

    let input = Rc::new(RefCell::new(interactions_input));
    let probe = Rc::new(RefCell::new(the_probe));
    let shared_num_interactions_per_item_trace =
        Rc::new(RefCell::new(num_interactions_per_item_trace));
    let shared_cooccurrences_trace = Rc::new(RefCell::new(cooccurrences_trace));
    let shared_similarities_trace = Rc::new(RefCell::new(jaccard_similarities_trace));

    // Listen on an address and call the closure for each connection
    listen("127.0.0.1:8000", |out| {
        Server {
            current_step: 0,
            out,
            worker: Rc::clone(&worker),
            input: Rc::clone(&input),
            probe: Rc::clone(&probe),
            shared_num_interactions_per_item_trace: Rc::clone(&shared_num_interactions_per_item_trace),
            shared_cooccurrences_trace: Rc::clone(&shared_cooccurrences_trace),
            shared_similarities_trace: Rc::clone(&shared_similarities_trace)
        }
    }).unwrap();
}
