use std::{
    cmp::min,
    collections::{HashMap, HashSet},
    future::Future,
    hash::Hash,
    sync::Arc,
    time::{Duration, Instant},
};

use dashmap::{mapref::entry::Entry, DashMap};

use super::{error::LoadBalanceError, LoadBalance, RequestHash};
use crate::{
    context::Endpoint,
    discovery::{Change, Discover, Instance},
    net::Address,
};

#[derive(Debug, Clone)]
pub struct ConsistentHashOption {
    /// If it is set to a value greater than 1, replicas will be used when connect to the primary
    /// node fails. This brings extra mem and cpu cost.
    /// If it is set to 1, error will be returned immediately when connect fails.
    replicas: usize,

    /// The number of virtual nodes corresponding to each real node
    /// The larger the value, the higher the memory and computational cost, and the more balanced
    /// the load When the number of nodes is large, it can be set smaller; conversely, it can be
    /// set larger The median VirtualFactor * Weight (if Weighted is true) is recommended to be
    /// around 1000 The recommended total number of virtual nodes is within 2000W
    virtual_factor: u32,

    /// Whether to follow Weight for load balancing
    /// If false, Weight is ignored for each instance, and VirtualFactor virtual nodes are
    /// generated for indiscriminate load balancing if true, Weight() * VirtualFactor virtual
    /// nodes are generated for each instance Note that for instance with weight 0, no virtual
    /// nodes will be generated regardless of the VirtualFactor number It is recommended to set
    /// it to true, but be careful to reduce the VirtualFactor appropriately
    weighted: bool,
}

impl ConsistentHashOption {
    pub fn new(replicas: usize, virtual_factor: u32, weighted: bool) -> Self {
        ConsistentHashOption {
            replicas,
            virtual_factor,
            weighted,
        }
    }
}

impl Default for ConsistentHashOption {
    fn default() -> Self {
        ConsistentHashOption {
            replicas: 1,
            virtual_factor: 100,
            weighted: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// RealNode is a wrapper of Instance
struct RealNode(Instance);

impl From<Instance> for RealNode {
    fn from(instance: Instance) -> Self {
        RealNode(instance)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// VirtualNode contains a RealNode and a hash value(which is get by hashing the RealNode's address
/// and serial number), and the virtualnode will be sorted by the hash value.
struct VirtualNode {
    real_node: Arc<RealNode>,
    hash: u64,
}

impl PartialOrd for VirtualNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.hash.partial_cmp(&other.hash)
    }
}

impl Ord for VirtualNode {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.hash.cmp(&other.hash)
    }
}

#[derive(Debug, Clone)]
struct WeightedInstances {
    real_nodes: Vec<Arc<RealNode>>,
    virtual_nodes: Vec<VirtualNode>,
}

#[derive(Debug)]
pub struct InstancePicker {
    shared_instances: Arc<WeightedInstances>,

    /// used for searching the virtual node
    request_hash: RequestHash,

    /// The index of the last selected virtual node
    last_pick: Option<usize>,

    /// The set of realnodes that have been selected
    used: HashSet<Address>,

    /// The number of replicas to pick, min(option.replicas, real_nodes.len())
    replicas: usize,
}

impl Iterator for InstancePicker {
    type Item = Address;

    fn next(&mut self) -> Option<Self::Item> {
        let virtual_nodes = &self.shared_instances.virtual_nodes;
        if self.shared_instances.real_nodes.is_empty() {
            return None;
        }

        // already picked all replicas
        if self.used.len() >= self.replicas {
            return None;
        }

        match self.last_pick {
            None => {
                // init states
                self.replicas = min(self.replicas, self.shared_instances.real_nodes.len());
                // find the first virtual node whose hash is greater than request_hash
                let mut index = virtual_nodes.partition_point(|vn| vn.hash < self.request_hash.0);
                if index == virtual_nodes.len() {
                    index = 0;
                }
                self.last_pick = Some(index);
                let addr = virtual_nodes
                    .get(index)
                    .unwrap()
                    .real_node
                    .0
                    .address
                    .clone();
                self.used.insert(addr.clone());
                Some(addr)
            }
            Some(last_pick) => {
                let mut index = last_pick;
                // find the next virtual node which is not used
                for _ in 0..virtual_nodes.len() {
                    index += 1;
                    if index == virtual_nodes.len() {
                        index = 0;
                    }
                    let addr = &virtual_nodes.get(index).unwrap().real_node.0.address;
                    if !self.used.contains(addr) {
                        self.last_pick = Some(index);
                        self.used.insert(addr.clone());
                        return Some(addr.clone());
                    }
                }
                None
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct ConsistentHashBalance<K>
where
    K: Hash + PartialEq + Eq + Send + Sync + 'static,
{
    option: ConsistentHashOption,
    router: DashMap<K, Arc<WeightedInstances>>,
    prev_hashs: DashMap<K, HashMap<Address, Vec<RequestHash>>>,
}

impl<K> ConsistentHashBalance<K>
where
    K: Hash + PartialEq + Eq + Send + Sync + 'static,
{
    pub fn with_discover<D>(&mut self, _: &D) -> &mut Self
    where
        D: Discover<Key = K>,
    {
        self
    }

    pub fn new(option: ConsistentHashOption) -> Self {
        Self {
            option,
            router: DashMap::new(),
            prev_hashs: DashMap::new(),
        }
    }

    fn build_weighted_instances(
        &self,
        instances: Vec<Arc<Instance>>,
    ) -> (WeightedInstances, HashMap<Address, Vec<RequestHash>>) {
        let mut real_nodes = Vec::with_capacity(instances.len());
        // total number of virtual nodes
        let virtual_factor = self.option.virtual_factor;
        let sum_of_nodes = if self.option.weighted {
            instances
                .iter()
                .fold(0, |lhs, rhs| lhs + (rhs.weight * virtual_factor) as usize)
        } else {
            instances.len() * virtual_factor as usize
        };
        let mut virtual_nodes = Vec::with_capacity(sum_of_nodes);
        let mut hashs = HashMap::new();
        let mut elapsed = Duration::new(0, 0);
        for instance in instances {
            let real_node = Arc::new(RealNode::from((*instance).clone()));
            real_nodes.push(real_node.clone());
            let mut weight = 1;
            if self.option.weighted {
                weight = instance.weight;
            }
            let str = instance.address.to_string();
            let vnode_lens = virtual_factor * weight;
            // try to reuse the buffer
            let mut buf = format!("{}#{}", str, vnode_lens).into_bytes();
            let mut sharp_pos = 0;
            for (i, bytei) in buf.iter().enumerate() {
                if *bytei == b'#' {
                    sharp_pos = i;
                    break;
                }
            }
            let start = Instant::now();
            hashs.insert(
                instance.address.clone(),
                Vec::with_capacity(vnode_lens as usize),
            );
            let hashcodes = hashs.get_mut(&instance.address).unwrap();
            elapsed += start.elapsed();
            for i in 0..(virtual_factor * weight) {
                let mut serial = i;
                let mut pos = buf.len();
                while serial > 0 {
                    pos -= 1;
                    buf[pos] = b'0' + (serial % 10) as u8;
                    serial /= 10;
                }
                for bytej in buf.iter_mut().take(pos).skip(sharp_pos + 1) {
                    *bytej = b'0';
                }
                // get address#i with leading zeros
                let hash = mur3::murmurhash3_x64_128(&buf, 0).0;
                virtual_nodes.push(VirtualNode {
                    real_node: real_node.clone(),
                    hash,
                });
                let start = Instant::now();
                hashcodes.push(RequestHash(hash));
                elapsed += start.elapsed();
            }
        }
        let start = Instant::now();
        virtual_nodes.sort_unstable();
        let end = Instant::now();
        let duration = end - start;
        println!("Sort time: {:?}ms", duration.as_millis());
        // let start = Instant::now();
        // // let mut hashs = HashMap::new();
        // for vnode in &virtual_nodes {
        //     let addr = vnode.real_node.0.address.clone();
        //     hashs
        //         .entry(addr)
        //         .or_insert_with(Vec::new)
        //         .push(RequestHash(vnode.hash));
        // }
        // let elapsed = start.elapsed();
        println!("record requesthash time: {:?} ms", elapsed.as_millis());
        (
            WeightedInstances {
                real_nodes,
                virtual_nodes,
            },
            hashs,
        )
    }

    fn rebuild<D: Discover>(
        &self,
        changes: Change<D::Key>,
        prev_hashs: &mut HashMap<Address, Vec<RequestHash>>,
        virtual_nodes: &Vec<VirtualNode>,
    ) -> Vec<VirtualNode> {
        // 1. get remove address from changes
        // 2. get all prev_hashs of remove address & merge them to a vector(please prealloc it) &
        // sort it, and please remove all prev_hashs of remove address from prev_hashs
        // 3. use Double Pointer, remove all virtual nodes which hash in the vector, both
        // virtual_nodes & prev_hashs are sorted, so you can do it in O(n) 4. get add
        // address from changes 5. build new virtual nodes from add address using
        // build_weighted_instances 6. merge new virtual nodes to virtual_nodes &
        // prev_hashs, please use double pointer, so you can do it in O(n) 7. return the
        // vector of new virtual nodes

        let start = Instant::now();
        let mut vnodes_removed: Vec<VirtualNode> = Vec::new();
        let mut remove_addrs = Vec::new();
        let mut add_ins = Vec::new();
        for addr in changes.removed {
            remove_addrs.push(addr.address.clone());
        }
        for addr in changes.added {
            add_ins.push(addr.clone());
        }
        let mut remove_prev_hashs = Vec::new();
        for addr in &remove_addrs {
            let mut prev_hash = prev_hashs.remove(&addr).unwrap();
            remove_prev_hashs.append(&mut prev_hash);
        }
        remove_prev_hashs.sort_unstable();
        let mut vnodes_ptr = 0;
        let mut remove_prev_hashs_ptr = 0;
        while remove_prev_hashs_ptr < remove_prev_hashs.len() && vnodes_ptr < virtual_nodes.len() {
            let vnode = &virtual_nodes[vnodes_ptr];
            let remove_prev_hash = remove_prev_hashs[remove_prev_hashs_ptr];
            if vnode.hash != remove_prev_hash.0 {
                vnodes_removed.push(vnode.clone());
            }
            if vnode.hash >= remove_prev_hash.0 {
                remove_prev_hashs_ptr += 1;
            }
            vnodes_ptr += 1;
        }
        let add_result = self.build_weighted_instances(add_ins);
        let (add_vnodes, add_hashs) = (add_result.0.virtual_nodes, add_result.1);
        let mut add_vnodes_ptr = 0;
        let mut new_vnodes_ptr = 0;
        let mut vnodes_added = Vec::new();
        while add_vnodes_ptr < add_vnodes.len() && new_vnodes_ptr < vnodes_removed.len() {
            let add_vnode = &add_vnodes[add_vnodes_ptr];
            let new_vnode = &vnodes_removed[new_vnodes_ptr];
            if add_vnode.hash < new_vnode.hash {
                vnodes_added.push(add_vnode.clone());
                add_vnodes_ptr += 1;
            } else {
                vnodes_added.push(new_vnode.clone());
                new_vnodes_ptr += 1;
            }
        }
        prev_hashs.extend(add_hashs.into_iter());
        let elapsed = start.elapsed();
        println!("Rebuild time: {:?} ms", elapsed.as_millis());
        vnodes_added
    }
}

impl<D> LoadBalance<D> for ConsistentHashBalance<D::Key>
where
    D: Discover,
{
    type InstanceIter = InstancePicker;

    type GetFut<'future> =
        impl Future<Output = Result<Self::InstanceIter, LoadBalanceError>> + Send + 'future
        where
            Self: 'future;

    fn get_picker<'future>(
        &'future self,
        endpoint: &'future Endpoint,
        discover: &'future D,
    ) -> Self::GetFut<'future>
    where
        Self: 'future,
    {
        async move {
            let request_hash = metainfo::METAINFO
                .try_with(|m| m.borrow().get::<RequestHash>().copied())
                .map_err(|_| LoadBalanceError::MissRequestHash)?;
            if request_hash.is_none() {
                return Err(LoadBalanceError::MissRequestHash);
            }
            let request_hash = request_hash.unwrap();
            let key = discover.key(endpoint);
            let weighted_list = match self.router.entry(key.clone()) {
                Entry::Occupied(e) => e.get().clone(),
                Entry::Vacant(e) => {
                    let instances = self.build_weighted_instances(
                        discover
                            .discover(endpoint)
                            .await
                            .map_err(|err| err.into())?,
                    );
                    self.prev_hashs.insert(key.clone(), instances.1);
                    e.insert(Arc::new(instances.0)).value().clone()
                }
            };
            Ok(InstancePicker {
                shared_instances: weighted_list,
                request_hash,
                last_pick: None,
                used: HashSet::new(),
                replicas: self.option.replicas,
            })
        }
    }

    fn rebalance(&self, changes: Change<<D as Discover>::Key>) {
        if let Entry::Occupied(entry) = self.router.entry(changes.key.clone()) {
            // entry.replace_entry(Arc::new(self.build_weighted_instances(changes.all)));
            if let Entry::Occupied(mut prev_hashs) = self.prev_hashs.entry(changes.key.clone()) {
                let new_nodes = self.rebuild::<D>(
                    changes,
                    &mut prev_hashs.get_mut(),
                    &(entry.get().virtual_nodes),
                );
                // entry.replace_entry(Arc::new());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        cell::RefCell,
        collections::{BTreeSet, HashMap},
        sync::Arc,
    };

    use futures::Future;
    use metainfo::{MetaInfo, METAINFO};
    use rand::Rng;

    use super::{ConsistentHashBalance, ConsistentHashOption, LoadBalance};
    use crate::{
        context::Endpoint,
        discovery::{Instance, StaticDiscover},
        loadbalance::{Change, Discover, RequestHash},
        net::Address,
    };

    fn empty_endpoint() -> Endpoint {
        Endpoint {
            service_name: faststr::FastStr::new_inline(""),
            address: None,
            tags: Default::default(),
        }
    }

    #[inline]
    fn set_request_hash(code: u64) {
        metainfo::METAINFO
            .try_with(|m| m.borrow_mut().insert(RequestHash(code)))
            .unwrap();
    }

    async fn test_with_meta_info<F, Fut>(f: F)
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = ()>,
    {
        let mi = MetaInfo::new();
        METAINFO
            .scope(RefCell::new(mi), async move {
                f().await;
            })
            .await;
    }

    // #[tokio::test]
    // async fn test_consistent_hash_balancer() {
    //     test_with_meta_info(|| consistent_hash_balancer_tests()).await;
    // }

    // async fn consistent_hash_balancer_tests() {
    //     let empty = empty_endpoint();
    //     let discover = StaticDiscover::from(vec![
    //         "127.0.0.1:8000".parse().unwrap(),
    //         "127.0.0.2:9000".parse().unwrap(),
    //     ]);
    //     let opt = ConsistentHashOption {
    //         replicas: 3,
    //         virtual_factor: 3,
    //         weighted: true,
    //     };
    //     let lb = ConsistentHashBalance::new(opt);
    //     set_request_hash(0);
    //     let picker = lb.get_picker(&empty, &discover).await.unwrap();
    //     let all = picker.collect::<Vec<_>>();
    //     assert_eq!(all.len(), 2);
    //     assert_ne!(all[0], all[1]);

    //     // Test replicas in ConsistentHashOption
    //     let opt = ConsistentHashOption {
    //         replicas: 1,
    //         virtual_factor: 3,
    //         weighted: true,
    //     };
    //     let lb = ConsistentHashBalance::new(opt);
    //     set_request_hash(0);
    //     let picker = lb.get_picker(&empty, &discover).await.unwrap();
    //     let all = picker.collect::<Vec<_>>();
    //     assert_eq!(all.len(), 1);
    // }

    // #[tokio::test]
    // async fn test_consistent_hash_consistent() {
    //     test_with_meta_info(|| consistent_hash_consistent_tests()).await;
    // }

    // async fn consistent_hash_consistent_tests() {
    //     // test if the same request key will get the same instances
    //     let empty = empty_endpoint();
    //     let instances = vec![
    //         Arc::new(Instance {
    //             address: Address::Ip("127.0.0.1:8000".parse().unwrap()),
    //             weight: 3,
    //             tags: Default::default(),
    //         }),
    //         Arc::new(Instance {
    //             address: Address::Ip("127.0.0.2:9000".parse().unwrap()),
    //             weight: 3,
    //             tags: Default::default(),
    //         }),
    //         Arc::new(Instance {
    //             address: Address::Ip("127.0.0.3:8800".parse().unwrap()),
    //             weight: 3,
    //             tags: Default::default(),
    //         }),
    //     ];
    //     let sum_weight = instances.iter().map(|i| i.weight).sum::<u32>();
    //     let opt = ConsistentHashOption {
    //         replicas: 2,
    //         virtual_factor: 3,
    //         weighted: true,
    //     };
    //     let discovery = StaticDiscover::new(instances.clone());
    //     let lb = ConsistentHashBalance::new(opt.clone());
    //     let weighted_instances = lb.build_weighted_instances(instances.clone());
    //     assert_eq!(
    //         weighted_instances.virtual_nodes.len(),
    //         (sum_weight * opt.virtual_factor) as usize
    //     );
    //     assert_eq!(weighted_instances.real_nodes.len(), instances.len());
    //     for _ in 0..100 {
    //         let request_hash = rand::random::<u64>();
    //         set_request_hash(request_hash);
    //         let picker = lb.get_picker(&empty, &discovery).await.unwrap();
    //         let all1 = picker.collect::<Vec<_>>();
    //         for _ in 0..3 {
    //             let picker2 = lb.get_picker(&empty, &discovery).await.unwrap();
    //             let all2 = picker2.collect::<Vec<_>>();
    //             assert_eq!(all1, all2);
    //         }
    //     }
    // }

    async fn simulate_random_picks(
        instances: Vec<Arc<Instance>>,
        times: usize,
    ) -> HashMap<Address, usize> {
        let mut map = HashMap::new();
        let empty = empty_endpoint();
        let opt = ConsistentHashOption {
            replicas: 3,
            virtual_factor: 100,
            weighted: true,
        };
        let discovery = StaticDiscover::new(instances.clone());
        let lb = ConsistentHashBalance::new(opt.clone());
        for _ in 0..times {
            let request_hash = rand::random::<u64>();
            set_request_hash(request_hash);
            let picker = lb.get_picker(&empty, &discovery).await.unwrap();
            let all = picker.collect::<Vec<_>>();
            for address in all {
                let count = map.entry(address).or_insert(0);
                *count += 1;
            }
        }
        map
    }

    fn new_instance(address: String, weight: u32) -> Arc<Instance> {
        Arc::new(Instance {
            address: Address::Ip(address.parse().unwrap()),
            weight,
            tags: Default::default(),
        })
    }

    // #[tokio::test]
    // async fn test_consistent_hash_balance() {
    //     test_with_meta_info(|| consistent_hash_balance_tests()).await;
    // }

    // async fn consistent_hash_balance_tests() {
    //     // TODO: Using standard deviation to evaluate load balancing is better?
    //     let mut rng = rand::thread_rng();
    //     let mut instances = vec![];
    //     for _ in 0..50 {
    //         let w = rng.gen_range(10..=100);
    //         let sub_net = rng.gen_range(0..=255);
    //         let port = rng.gen_range(1000..=65535);
    //         instances.push(new_instance(format!("172.17.0.{}:{}", sub_net, port), w));
    //         instances.push(new_instance(format!("192.168.32.{}:{}", sub_net, port), w));
    //     }
    //     let result = simulate_random_picks(instances.clone(), 1000000).await;
    //     let sum_visits = result.values().sum::<usize>();
    //     let sum_weight = instances.iter().map(|i| i.weight).sum::<u32>();
    //     let mut deviation = 0.0;
    //     let mut max_eps: f64 = 0.0;
    //     for instance in instances.iter() {
    //         let count: usize = *(result.get(&(instance.address)).unwrap_or(&0));
    //         let exact = count as f64;
    //         let expect = instance.weight as f64 / sum_weight as f64 * sum_visits as f64;
    //         let eps = (exact - expect).abs() / expect;
    //         // compute the standard deviation
    //         deviation = deviation + (eps * eps);
    //         max_eps = max_eps.max(eps);
    //     }
    //     println!("max_eps: {}", max_eps);
    //     println!(
    //         "standard deviation: {}",
    //         (deviation / instances.len() as f64).sqrt()
    //     );
    //     assert!(max_eps < 0.1);
    // }

    #[tokio::test]
    async fn test_consistent_hash_change() {
        test_with_meta_info(|| consistent_hash_change_tests()).await;
    }

    async fn consistent_hash_change_tests() {
        let mut instances = vec![];
        let opt = ConsistentHashOption {
            replicas: 1,
            virtual_factor: 100,
            weighted: true,
        };
        let mut rng = rand::thread_rng();
        for i in 0..200 {
            let w = rng.gen_range(100..=100);
            let port = rng.gen_range(1000..=65535);
            instances.push(new_instance(format!("127.0.0.{}:{}", i, port), w));
            instances.push(new_instance(format!("192.168.0.{}:{}", i, port), w));
        }
        let discovery = StaticDiscover::new(instances.clone());
        let mut lb = ConsistentHashBalance::new(opt.clone());
        lb.with_discover(&discovery);
        let mut virtual_result = lb.build_weighted_instances(instances.clone());
        // construst a change from instances
        // 1. new a change
        // 2. remove 2 instances randomly and add them to change's removed
        // 3. create 2 new instances randomly and add them to change's added
        let mut disc = StaticDiscover::new(instances.clone());
        let mut change: Change<<StaticDiscover as Discover>::Key> = Change {
            key: (),
            all: Vec::new(),
            added: Vec::new(),
            removed: Vec::new(),
            updated: Vec::new(),
        };
        let mut remove_index = rng.gen_range(0..instances.len());
        let mut remove_instance = instances.remove(remove_index);
        change.removed.push(remove_instance.clone());
        remove_index = rng.gen_range(0..instances.len());
        remove_instance = instances.remove(remove_index);
        change.removed.push(remove_instance.clone());
        let mut add_instance = new_instance("127.0.2.1:8000".to_string(), 100);
        change.added.push(add_instance.clone());
        lb.rebuild::<StaticDiscover>(
            change,
            &mut virtual_result.1,
            &mut virtual_result.0.virtual_nodes,
        );
        // let virtual_nodes: BTreeSet<_> = virtual_nodes.into_iter().collect();

        // let remove_index = rng.gen_range(0..instances.len());
        // let _remove_instance = instances.remove(remove_index);
        // let new_virtual_nodes = lb.build_weighted_instances(instances.clone()).virtual_nodes;
        // for node in new_virtual_nodes {
        //     assert!(virtual_nodes.contains(&node));
        // }
    }
}
