use std::rc::Rc;

use differential_dataflow::operators::arrange::TraceAgent;
use differential_dataflow::trace::implementations::spine_fueled::Spine;
use differential_dataflow::trace::implementations::ord::OrdValBatch;


pub type Trace<K, V> = TraceAgent<Spine<K, V, usize, isize, Rc<OrdValBatch<K, V, usize, isize>>>>;