extern crate timely;

use std::cell::RefCell;
use std::rc::Rc;
use std::iter;
use std::collections::HashMap;

use timely::communication::allocator::thread::Thread;
use timely::dataflow::operators::probe::Handle;
use timely::worker::Worker;

use differential_dataflow::input::InputSession;

use crate::types::Trace;

use differential_dataflow::operators::arrange::ArrangeByKey;
use timely::dataflow::operators::Probe;
use timely::dataflow::ProbeHandle;
use differential_dataflow::operators::join::JoinCore;
use differential_dataflow::operators::{Join, CountTotal, Count, Reduce};

pub fn dataflow_for_recommender(
    mut worker: Worker<Thread>
) -> (InputSession<usize, (u32, u32), isize>,
      InputSession<usize, (u32, u32), isize>,
      ProbeHandle<usize>,
      Trace<u32, isize>,
      Trace<(u32, u32), isize>,
      Trace<(u32, u32), String>,
      Trace<u32, u32>,
) {

    let mut interactions_input: InputSession<usize, (u32, u32), isize> = InputSession::new();
    let mut query_input: InputSession<usize, (u32, u32), isize> = InputSession::new();

    let mut probe = Handle::new();

    let (num_interactions_per_item_trace, cooccurrences_trace,
        similarities_trace, recommendations_trace) =
        worker.dataflow(|scope| {

            let interactions = interactions_input.to_collection(scope);

            let num_interactions_per_item = interactions
                .map(|(_user, item)| item)
                .count_total();

            let arranged_remaining_interactions = interactions.arrange_by_key();

            // Compute the number of cooccurrences of each item pair
            let cooccurrences = arranged_remaining_interactions
                .join_core(&arranged_remaining_interactions, |_user, &item_a, &item_b| {
                    if item_a > item_b { Some((item_a, item_b)) } else { None }
                })
                .count();

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
                // Compute Jaccard similarity, has to be done in a map due to the lack of a
                // total order for f64 (which seems to break the consolidation in join)
                .map(|((item_a, item_b), (num_cooc, occ_a, occ_b))| {
                    let jaccard = num_cooc as f64 / (occ_a + occ_b - num_cooc) as f64;
                    ((item_a, item_b), jaccard.to_string())
                });

            let arranged_jaccard_similarities = jaccard_similarities.arrange_by_key();

            let queries = query_input.to_collection(scope);

            let bidirectional_similarities = jaccard_similarities
                .flat_map(|((item_a, item_b), similarity_str)| {
                    let similarity = (similarity_str.parse::<f64>().unwrap() * 10000_f64) as u64;

                    iter::once((item_a, (item_b, similarity)))
                        .chain(iter::once((item_b, (item_a, similarity))))
                });

            let queries_by_item = queries
                .map(|(query, item)| (item, query));

            let recommendations = queries_by_item
                .join_map(
                    &bidirectional_similarities,
                    |_history_item, query, (other_item, similarity)| {
                        (*query, (*other_item, *similarity))
                    })
                .reduce(|_query, items_with_similarities, output| {

                    let mut similarities_per_item: HashMap<u32, isize> = HashMap::new();

                    for ((item, similarity), mult) in items_with_similarities.iter() {
                        let entry = similarities_per_item.entry(*item).or_insert(0_isize);
                        *entry += (*similarity as isize) * *mult;
                    }

                    let (recommended_item, _) = similarities_per_item
                        .iter()
                        .max_by(|(_, sim_a), (_, sim_b)| sim_a.cmp(sim_b))
                        .unwrap();

                    output.push((*recommended_item, 1_isize));
                });

            let arranged_recommendations = recommendations
                .arrange_by_key();

            arranged_recommendations.stream.probe_with(&mut probe);


            (arranged_num_interactions_per_item.trace, arranged_cooccurrences.trace,
             arranged_jaccard_similarities.trace, arranged_recommendations.trace)
        });

    (interactions_input,
     query_input,
     probe,
     num_interactions_per_item_trace,
     cooccurrences_trace,
     similarities_trace,
     recommendations_trace)
}